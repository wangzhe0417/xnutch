package org.apache.nutch.crawl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.util.IPUtils;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Random;

/**
 * robots.txt本地缓存流程主机注入
 *
 * @author wangzhe
 * @create 2016-08-15-10:00
 */

public class RobotsInjector extends Configured implements Tool {
  public static final Logger LOG = LoggerFactory.getLogger(RobotsInjector.class);

  public static class InjectMapper
      implements Mapper<Text, CrawlDatum, Text, CrawlDatum> {
    private int interval;
    private JobConf jobConf;
    private long curTime;

    public void configure(JobConf job) {
      this.jobConf = job;
      interval = jobConf.getInt("db.fetch.interval.default", 2592000);
      curTime = job.getLong("injector.current.time",
          System.currentTimeMillis());
    }

    public void close() {
    }

    public void map(Text key, CrawlDatum value,
        OutputCollector<Text, CrawlDatum> output, Reporter reporter)
        throws IOException {

      reporter.getCounter("RobotsInjector", "host_scanned").increment(1);

      CrawlDatum datum = new CrawlDatum();
      datum.setStatus(CrawlDatum.STATUS_INJECTED);
      datum.setFetchInterval(interval);
      datum.setFetchTime(curTime);
      datum.setScore(value.getScore());

      InetAddress[] ips = IPUtils.getIPs(value);
      if (ips != null)
        IPUtils.setIP(datum, ips);

      output.collect(key, datum);
    }
  }

  public static class MergeReducer
      implements Reducer<Text, CrawlDatum, Text, CrawlDatum> {
    private int interval;
    private boolean overwrite = false;
    private boolean update = false;

    public void configure(JobConf job) {
      // TODO: 主机的这些属性能和链接公用吗？
      interval = job.getInt("db.fetch.interval.default", 2592000);
      overwrite = job.getBoolean("hostdb.injector.overwrite", false);
      update = job.getBoolean("hostdb.injector.update", true);
      LOG.info("RobotsInjector: overwrite: " + overwrite);
      LOG.info("RobotsInjector: update: " + update);
    }

    public void close() {
    }

    private CrawlDatum old = new CrawlDatum();
    private CrawlDatum injected = new CrawlDatum();

    public void reduce(Text key, Iterator<CrawlDatum> values,
        OutputCollector<Text, CrawlDatum> output, Reporter reporter)
        throws IOException {
      boolean oldSet = false;
      boolean injectedSet = false;
      while (values.hasNext()) {
        CrawlDatum val = values.next();
        if (val.getStatus() == CrawlDatum.STATUS_INJECTED) {
          injected.set(val);
          injected.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
          injectedSet = true;
        } else {
          old.set(val);
          oldSet = true;
        }

      }

      CrawlDatum res = null;

      // Old default behaviour
      if (injectedSet && !oldSet) {
        res = injected;
      } else {
        res = old;
      }
      if (injectedSet && oldSet) {
        reporter.getCounter("RobotsInjector", "robots_merged").increment(1);
      }
      /**
       * Whether to overwrite, ignore or update existing records
       *
       * @see https://issues.apache.org/jira/browse/NUTCH-1405
       */
      // Injected record already exists and update but not overwrite
      if (injectedSet && oldSet && update && !overwrite) {
        res = old;
        old.putAllMetaData(injected);

        // 每一次RobotsInjector都是遍历整个HostDb的操作，不是增量操作,所以直接覆盖Score
        old.setScore(injected.getScore());

        old.setFetchInterval(injected.getFetchInterval() != interval
            ? injected.getFetchInterval() : old.getFetchInterval());
      }

      // Injected record already exists and overwrite
      if (injectedSet && oldSet && overwrite) {
        res = injected;
      }

      output.collect(key, res);
    }
  }

  public RobotsInjector() {
  }

  public RobotsInjector(Configuration conf) {
    setConf(conf);
  }

  public void inject(Path robotsDb, Path hostDb) throws IOException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    if (LOG.isInfoEnabled()) {
      LOG.info("RobotsInjector: starting at " + sdf.format(start));
      LOG.info("RobotsInjector: robotsDb: " + robotsDb);
      LOG.info("RobotsInjector: hostDb: " + hostDb);
    }

    Path tempDir = new Path(
        getConf().get("mapred.temp.dir", ".") + "/inject-temp-"
            + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    // map text input file to a <host,CrawlDatum> file
    if (LOG.isInfoEnabled()) {
      LOG.info("RobotsInjector: Converting injected host to robots db entries.");
    }

    FileSystem fs = FileSystem.get(getConf());
    // determine if the robotsdb already exists
     boolean dbExists = fs.exists(robotsDb);

    JobConf extractJob = new NutchJob(getConf());
    extractJob.setJobName("inject " + hostDb);

    FileInputFormat.addInputPath(extractJob,
        new Path(hostDb, HostDb.CURRENT_NAME));

    extractJob.setInputFormat(SequenceFileInputFormat.class);
    extractJob.setMapperClass(InjectMapper.class);
    FileOutputFormat.setOutputPath(extractJob, tempDir);
    if (dbExists) {
      // Don't run merge injected hosts, wait for merge with
      // existing DB
      extractJob.setOutputFormat(SequenceFileOutputFormat.class);
      extractJob.setNumReduceTasks(0);
    } else {
      extractJob.setOutputFormat(MapFileOutputFormat.class);
      extractJob.setReducerClass(MergeReducer.class);
      extractJob.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs",
          false);
    }
    extractJob.setOutputKeyClass(Text.class);
    extractJob.setOutputValueClass(CrawlDatum.class);
    extractJob.setLong("injector.current.time", System.currentTimeMillis());

    RunningJob mapJob = null;
    try {
      mapJob = JobClient.runJob(extractJob);
    } catch (IOException e) {
      fs.delete(tempDir, true);
      throw e;
    }

    long hostsInjected = mapJob.getCounters()
        .findCounter("RobotsInjector", "host_scanned").getValue();
    LOG.info("RobotsInjector: Total number of hosts scanned: " + hostsInjected);

    // 第二阶段MapReduce操作，如果存在RobotsDb则合并tempDir与RobotsDb
    long robotsMerged = 0;
    if (dbExists) {
      // merge with existing robotsdb
      if (LOG.isInfoEnabled()) {
        LOG.info("RobotsInjector: Merging extracted hosts into robotsdb.");
      }
      JobConf mergeJob = HostDb.createJob(getConf(), robotsDb);
      FileInputFormat.addInputPath(mergeJob, tempDir);
      mergeJob.setReducerClass(MergeReducer.class);
      try {
        RunningJob merge = JobClient.runJob(mergeJob);
        robotsMerged = merge.getCounters()
            .findCounter("RobotsInjector", "robots_merged").getValue();
        LOG.info("RobotsInjector: hosts merged: " + robotsMerged);
      } catch (IOException e) {
        fs.delete(tempDir, true);
        throw e;
      }
      HostDb.install(mergeJob, robotsDb);
    } else {
      if (LOG.isInfoEnabled()) {
        LOG.info("RobotsInjector: Total number of hosts injected: " + hostsInjected);
      }
      HostDb.install(extractJob, robotsDb);
    }

    // clean up
    fs.delete(tempDir, true);
     LOG.info("RobotsInjector: Total new hosts injected: "
     + (hostsInjected - robotsMerged));
    long end = System.currentTimeMillis();
    LOG.info("RobotsInjector: finished at " + sdf.format(end) + ", elapsed: "
        + TimingUtil.elapsedTime(start, end));
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new RobotsInjector(),
        args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: RobotsInjector <robotsdb> <hostdb>");
      return -1;
    }
    try {
      inject(new Path(args[0]), new Path(args[1]));
      return 0;
    } catch (Exception e) {
      LOG.error("RobotsInjector: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

}
