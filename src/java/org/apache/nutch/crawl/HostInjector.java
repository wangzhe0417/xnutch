package org.apache.nutch.crawl;

import java.io.*;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.*;

// Commons Logging imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.nutch.net.*;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;

/**
 * 从CrawlDb中提取链接主机并注入到HostDb。
 * 
 * @author kidden
 *
 */
public class HostInjector extends Configured implements Tool {
  public static final Logger LOG = LoggerFactory.getLogger(HostInjector.class);

  // TODO: 是否可以考虑从主机种子文件注入？若是，以下配置有用。

  /** 过滤和规范化链接，提取链接主机 */
  public static class InjectMapper
      implements Mapper<Text, CrawlDatum, Text, CrawlDatum> {
    private URLNormalizers urlNormalizers;
    private int interval;
    private JobConf jobConf;
    private URLFilters filters;
    private long curTime;
    private boolean isNormalize;
    private boolean isFilter;

    public void configure(JobConf job) {
      this.jobConf = job;
      isFilter = jobConf.getBoolean("host.inject.isFilter", false);
      isNormalize = jobConf.getBoolean("host.inject.isNormalize", false);
      if (isNormalize) {
        urlNormalizers = new URLNormalizers(job, URLNormalizers.SCOPE_INJECT);
      }
      if (isFilter) {
        filters = new URLFilters(jobConf);
      }
      interval = jobConf.getInt("db.fetch.interval.default", 2592000);
      curTime = job.getLong("injector.current.time",
          System.currentTimeMillis());
    }

    public void close() {
    }

    public void map(Text key, CrawlDatum value,
        OutputCollector<Text, CrawlDatum> output, Reporter reporter)
        throws IOException {
      String url = key.toString().trim(); // value is line of text

      if (url != null && (url.length() == 0 || url.startsWith("#"))) {
        /* Ignore line that start with # */
        return;
      }

      reporter.getCounter("HostInjector", "urls_scanned").increment(1);

      try {
        if (isNormalize) {
          url = urlNormalizers.normalize(url, URLNormalizers.SCOPE_INJECT);
        }
        if (isFilter) {
          url = filters.filter(url); // filter the url
        }
      } catch (Exception e) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("Skipping " + url + ":" + e);
        }
        url = null;
      }
      if (url == null) {
        reporter.getCounter("HostInjector", "urls_filtered").increment(1);
      } else { // if it passes
        String host = new URL(url).getHost();

        CrawlDatum datum = new CrawlDatum();
        datum.setStatus(CrawlDatum.STATUS_INJECTED);
        // XXX: 间隔设置有问题，采用主机解析初始间隔
        datum.setFetchInterval(interval);
        datum.setFetchTime(curTime);
        datum.setScore(value.getScore());

        output.collect(new Text(host), datum);
      }
    }
  }

  public static class InjectReducer
      implements Reducer<Text, CrawlDatum, Text, CrawlDatum> {

    public void configure(JobConf job) {
    }

    public void close() {
    }

    private CrawlDatum datum = new CrawlDatum();

    public void reduce(Text key, Iterator<CrawlDatum> values,
        OutputCollector<Text, CrawlDatum> output, Reporter reporter)
        throws IOException {
      float scores = 0;
      while (values.hasNext()) {
        CrawlDatum val = values.next();
        scores += val.getScore();
        datum.set(val);
      }
      datum.setScore(scores);

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
      LOG.info("HostInjector: overwrite: " + overwrite);
      LOG.info("HostInjector: update: " + update);
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
        reporter.getCounter("HostInjector", "hosts_merged").increment(1);
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

        // 每一次HostInjector都是遍历整个CrawlDb的操作，不是增量操作,所以直接覆盖
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

  public HostInjector() {
  }

  public HostInjector(Configuration conf) {
    setConf(conf);
  }

  public void inject(Path crawlDb, Path hostDb) throws IOException {
    inject(crawlDb, hostDb, false, false);

  }

  public void inject(Path crawlDb, Path hostDb, boolean isFilter,
      boolean isNormalize) throws IOException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    if (LOG.isInfoEnabled()) {
      LOG.info("HostInjector: starting at " + sdf.format(start));
      LOG.info("HostInjector: crawlDb: " + crawlDb);
      LOG.info("HostInjector: hostDb: " + hostDb);
      LOG.info("HostInjector:filtering:" + isFilter);
      LOG.info("HostInjector:normalizing:" + isNormalize);
    }

    Path tempDir = new Path(
        getConf().get("mapred.temp.dir", ".") + "/inject-temp-"
            + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
    getConf().setBoolean("host.inject.isFilter", isFilter);
    getConf().setBoolean("host.inject.isNormalize", isNormalize);
    if (LOG.isInfoEnabled()) {
      LOG.info("HostInjector: Converting urls to host entries.");
    }

    FileSystem fs = FileSystem.get(getConf());

    // 第一阶段MapReduce操作，将CrawlDb数据转换为<host,datum>，并计算host对应分数
    JobConf extractJob = new NutchJob(getConf());
    extractJob.setJobName("inject host from " + crawlDb);

    extractJob.setInputFormat(SequenceFileInputFormat.class);
    FileInputFormat.addInputPath(extractJob,
        new Path(crawlDb, CrawlDb.CURRENT_NAME));
    extractJob.setMapperClass(InjectMapper.class);
    extractJob.setReducerClass(InjectReducer.class);
    extractJob.setOutputFormat(MapFileOutputFormat.class);
    FileOutputFormat.setOutputPath(extractJob, tempDir);
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
    long urlsInjected = mapJob.getCounters()
        .findCounter("HostInjector", "urls_scanned").getValue();
    long urlsFiltered = mapJob.getCounters()
        .findCounter("HostInjector", "urls_filtered").getValue();
    LOG.info("HostInjector: Total number of urls rejected by filters: "
        + urlsFiltered);
    LOG.info("HostInjector: Total number of urls scanned: " + urlsInjected);

    // 第二阶段MapReduce操作，如果存在hostDb则合并injectHost与HostDb
    long hostsMerged = 0;

    if (LOG.isInfoEnabled()) {
      LOG.info("HostInjector: Merging extracted hosts into host db.");
    }
    JobConf mergeJob = HostDb.createJob(getConf(), hostDb);

    FileInputFormat.addInputPath(mergeJob, tempDir);

    // 使用缺省的Mapper,原样输出
    mergeJob.setReducerClass(MergeReducer.class);
    try {
      RunningJob merge = JobClient.runJob(mergeJob);
      hostsMerged = merge.getCounters()
          .findCounter("HostInjector", "hosts_merged").getValue();
      LOG.info("HostInjector: HOSTs merged: " + hostsMerged);
    } catch (IOException e) {
      fs.delete(tempDir, true);
      throw e;
    }
    HostDb.install(mergeJob, hostDb);

    // clean up
    fs.delete(tempDir, true);
    long end = System.currentTimeMillis();
    LOG.info("HostInjector: finished at " + sdf.format(end) + ", elapsed: "
        + TimingUtil.elapsedTime(start, end));
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new HostInjector(),
        args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println(
          "Usage: HostInjector <crawldb> <hostdb>[-filter][-normalize]");
      return -1;
    }

    boolean isFilter = false;
    boolean isNormalize = false;

    for (int i = 0; i < args.length; i++) {
      if ("-filter".equals(args[i])) {
        isFilter = true;
      }
      if ("-normalize".equals(args[i])) {
        isNormalize = true;
      }
    }

    try {
      inject(new Path(args[0]), new Path(args[1]), isFilter, isNormalize);
      return 0;
    } catch (Exception e) {
      LOG.error("HostInjector: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

}
