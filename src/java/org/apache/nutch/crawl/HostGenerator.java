
package org.apache.nutch.crawl;

import java.io.*;
import java.util.*;
import java.text.*;

// rLogging imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.LockUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;

/**
 * 待解析主机列表生成。
 * 
 **/
public class HostGenerator extends Configured implements Tool {

  public static final Logger LOG = LoggerFactory.getLogger(HostGenerator.class);

  // TODO: 使用新的配置属性名，不和链接混用
  public static final String GENERATE_UPDATE_CRAWLDB = "generate.update.crawldb";
  public static final String GENERATOR_MIN_SCORE = "generate.min.score";
  public static final String GENERATOR_MIN_INTERVAL = "generate.min.interval";
  public static final String GENERATOR_RESTRICT_STATUS = "generate.restrict.status";
  public static final String GENERATOR_FILTER = "generate.filter";
  public static final String GENERATOR_NORMALISE = "generate.normalise";
  public static final String GENERATOR_MAX_COUNT = "generate.max.count";
  public static final String GENERATOR_COUNT_MODE = "generate.count.mode";
  public static final String GENERATOR_COUNT_VALUE_DOMAIN = "domain";
  public static final String GENERATOR_COUNT_VALUE_HOST = "host";
  public static final String GENERATOR_TOP_N = "generate.topN";
  public static final String GENERATOR_CUR_TIME = "generate.curTime";
  public static final String GENERATOR_DELAY = "crawl.gen.delay";
  public static final String GENERATOR_MAX_NUM_SEGMENTS = "generate.max.num.segments";
  
  // deprecated parameters 
  public static final String GENERATE_MAX_PER_HOST_BY_IP = "generate.max.per.host.by.ip";

  public static class SelectorEntry implements Writable {
    public Text host;
    public CrawlDatum datum;
    public IntWritable segnum;

    public SelectorEntry() {
      host = new Text();
      datum = new CrawlDatum();
      segnum = new IntWritable(0);
    }

    public void readFields(DataInput in) throws IOException {
      host.readFields(in);
      datum.readFields(in);
      segnum.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
      host.write(out);
      datum.write(out);
      segnum.write(out);
    }

    public String toString() {
      return "host=" + host.toString() + ", datum=" + datum.toString() + ", segnum="
          + segnum.toString();
    }
  }

  /** Selects entries due for fetch. */
  public static class Selector implements
      Mapper<Text,CrawlDatum,FloatWritable,SelectorEntry>,
      Partitioner<FloatWritable,Writable>,
      Reducer<FloatWritable,SelectorEntry,Text,CrawlDatum> {
    private LongWritable genTime = new LongWritable(System.currentTimeMillis());
    private long curTime;
    private long limit;
    private long count;
    private int maxCount;
    /// TODO: 通过域名进行划分计数，有意义
    private boolean byDomain = false;
    private Partitioner<Text,Writable> partitioner = new URLPartitioner();

    private ScoringFilters scfilters;
    private SelectorEntry entry = new SelectorEntry();
    private FloatWritable sortValue = new FloatWritable();

    private long genDelay;
    private FetchSchedule schedule;
    private float scoreThreshold = 0f;
    private int intervalThreshold = -1;
    private String restrictStatus = null;
//    private int maxNumSegments = 1;
    int currentsegmentnum = 1;

    public void configure(JobConf job) {
      curTime = job.getLong(GENERATOR_CUR_TIME, System.currentTimeMillis());
      limit = job.getLong(GENERATOR_TOP_N, Long.MAX_VALUE) / job.getNumReduceTasks();
      maxCount = job.getInt(GENERATOR_MAX_COUNT, -1);
      if (maxCount==-1){
        byDomain = false;
      }
      if (GENERATOR_COUNT_VALUE_DOMAIN.equals(job.get(GENERATOR_COUNT_MODE))) byDomain = true;

      scfilters = new ScoringFilters(job);
      partitioner.configure(job);
      genDelay = job.getLong(GENERATOR_DELAY, 7L) * 3600L * 24L * 1000L;
      long time = job.getLong(Nutch.GENERATE_TIME_KEY, 0L);
      if (time > 0) genTime.set(time);
      schedule = FetchScheduleFactory.getFetchSchedule(job);
      scoreThreshold = job.getFloat(GENERATOR_MIN_SCORE, Float.NaN);
      intervalThreshold = job.getInt(GENERATOR_MIN_INTERVAL, -1);
      restrictStatus = job.get(GENERATOR_RESTRICT_STATUS, null);
    }

    public void close() {}

    /** Select & invert subset due for fetch. */
    public void map(Text key, CrawlDatum value,
        OutputCollector<FloatWritable,SelectorEntry> output, Reporter reporter)
        throws IOException {
      Text host = key;
      CrawlDatum crawlDatum = value;

      // check fetch schedule
      if (!schedule.shouldFetch(host, crawlDatum, curTime)) {
        LOG.debug("-shouldFetch rejected '" + host + "', fetchTime="
            + crawlDatum.getFetchTime() + ", curTime=" + curTime);
        return;
      }

      LongWritable oldGenTime = (LongWritable) crawlDatum.getMetaData().get(
          Nutch.WRITABLE_GENERATE_TIME_KEY);
      if (oldGenTime != null) { // awaiting fetch & update
        if (oldGenTime.get() + genDelay > curTime) // still wait for
        // update
        return;
      }
      float sort = 1.0f;
      try {
        sort = scfilters.generatorSortValue(key, crawlDatum, sort);
      } catch (ScoringFilterException sfe) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("Couldn't filter generatorSortValue for " + key + ": " + sfe);
        }
      }

      if (restrictStatus != null
        && !restrictStatus.equalsIgnoreCase(CrawlDatum.getStatusName(crawlDatum.getStatus()))) return;

      // consider only entries with a score superior to the threshold
      if (scoreThreshold != Float.NaN && sort < scoreThreshold) return;

      // consider only entries with a retry (or fetch) interval lower than threshold
      if (intervalThreshold != -1 && crawlDatum.getFetchInterval() > intervalThreshold) return;

      // sort by decreasing score, using DecreasingFloatComparator
      sortValue.set(sort);
      // record generation time
      crawlDatum.getMetaData().put(Nutch.WRITABLE_GENERATE_TIME_KEY, genTime);
      entry.datum = crawlDatum;
      entry.host = key;
      output.collect(sortValue, entry); // invert for sort by score
    }

    /** Partition by host / domain or IP. */
    public int getPartition(FloatWritable key, Writable value, int numReduceTasks) {
      return (((SelectorEntry) value).host.toString().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
      //return partitioner.getPartition(((SelectorEntry) value).host, key, numReduceTasks);
    }

    /** Collect until limit is reached. */
    public void reduce(FloatWritable key, Iterator<SelectorEntry> values,
        OutputCollector<Text,CrawlDatum> output, Reporter reporter)
        throws IOException {

      while (values.hasNext()) {

        if (count == limit) {
        	break;
        }

        SelectorEntry entry = values.next();

        output.collect(entry.host, entry.datum);

        // Count is incremented only when we keep the URL
        // maxCount may cause us to skip it.
        count++;
      }
    }
  }

  public static class DecreasingFloatComparator extends FloatWritable.Comparator {

    /** Compares two FloatWritables decreasing. */
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return super.compare(b2, s2, l2, b1, s1, l1);
    }
  }

  public static class SelectorInverseMapper extends MapReduceBase implements
      Mapper<FloatWritable,SelectorEntry,Text,SelectorEntry> {

    public void map(FloatWritable key, SelectorEntry value,
        OutputCollector<Text,SelectorEntry> output, Reporter reporter) throws IOException {
      SelectorEntry entry = value;
      output.collect(entry.host, entry);
    }
  }

  public static class PartitionReducer extends MapReduceBase implements
      Reducer<Text,SelectorEntry,Text,CrawlDatum> {

    public void reduce(Text key, Iterator<SelectorEntry> values,
        OutputCollector<Text,CrawlDatum> output, Reporter reporter) throws IOException {
      // if using HashComparator, we get only one input key in case of
      // hash collision
      // so use only URLs from values
      while (values.hasNext()) {
        SelectorEntry entry = values.next();
        output.collect(entry.host, entry.datum);
      }
    }

  }

  /** Sort fetch lists by hash of URL. */
  public static class HashComparator extends WritableComparator {
    public HashComparator() {
      super(Text.class);
    }

    @SuppressWarnings("rawtypes" )
    public int compare(WritableComparable a, WritableComparable b) {
      Text url1 = (Text) a;
      Text url2 = (Text) b;
      int hash1 = hash(url1.getBytes(), 0, url1.getLength());
      int hash2 = hash(url2.getBytes(), 0, url2.getLength());
      return (hash1 < hash2 ? -1 : (hash1 == hash2 ? 0 : 1));
    }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      int hash1 = hash(b1, s1, l1);
      int hash2 = hash(b2, s2, l2);
      return (hash1 < hash2 ? -1 : (hash1 == hash2 ? 0 : 1));
    }

    private static int hash(byte[] bytes, int start, int length) {
      int hash = 1;
      // make later bytes more significant in hash code, so that sorting
      // by
      // hashcode correlates less with by-host ordering.
      for (int i = length - 1; i >= 0; i--)
        hash = (31 * hash) + (int) bytes[start + i];
      return hash;
    }
  }

  /**
   * Update the HostDB so that the next generate won't include the same HOSTs.
   */
  public static class CrawlDbUpdater extends MapReduceBase implements
      Mapper<Text,CrawlDatum,Text,CrawlDatum>, Reducer<Text,CrawlDatum,Text,CrawlDatum> {
    long generateTime;

    public void configure(JobConf job) {
      generateTime = job.getLong(Nutch.GENERATE_TIME_KEY, 0L);
    }

    public void map(Text key, CrawlDatum value, OutputCollector<Text,CrawlDatum> output,
        Reporter reporter) throws IOException {
      output.collect(key, value);
    }

    private CrawlDatum orig = new CrawlDatum();
    private LongWritable genTime = new LongWritable(0L);

    public void reduce(Text key, Iterator<CrawlDatum> values,
        OutputCollector<Text,CrawlDatum> output, Reporter reporter) throws IOException {
      genTime.set(0L);
      while (values.hasNext()) {
        CrawlDatum val = values.next();
        if (val.getMetaData().containsKey(Nutch.WRITABLE_GENERATE_TIME_KEY)) {
          LongWritable gt = (LongWritable) val.getMetaData().get(
              Nutch.WRITABLE_GENERATE_TIME_KEY);
          genTime.set(gt.get());
          if (genTime.get() != generateTime) {
            orig.set(val);
            genTime.set(0L);
            continue;
          }
        } else {
          orig.set(val);
        }
      }
      if (genTime.get() != 0L) {
        orig.getMetaData().put(Nutch.WRITABLE_GENERATE_TIME_KEY, genTime);
      }
      output.collect(key, orig);
    }
  }

  public HostGenerator() {}

  public HostGenerator(Configuration conf) {
    setConf(conf);
  }

  public Path generate(Path dbDir, Path segments, int numLists, long topN, long curTime)
      throws IOException {
    return generate(dbDir, segments, numLists, topN, curTime, false);
  }

  /**
   * 
   * @param dbDir
   *          Host database directory
   * @param segments
   *          Host Segments directory
   * @param numLists
   *          Number of reduce tasks
   * @param topN
   *          Number of top HOSTs to be selected
   * @param curTime
   *          Current time in milliseconds
   * 
   * @return Path to generated segment or null if no entries were selected
   * 
   * @throws IOException
   *           When an I/O error occurs
   */
  public Path generate(Path dbDir, Path segments, int numLists, long topN,
      long curTime, boolean force)
      throws IOException {

//    Path tempDir = new Path(getConf().get("mapred.temp.dir", ".") + "/generate-temp-"
//        + java.util.UUID.randomUUID().toString());
	  
	  Path segment = new Path(segments, generateSegmentName());
	   Path output = new Path(segment, CrawlDatum.GENERATE_DIR_NAME);

	    LOG.info("HostGenerator: segment: " + segment);

    Path lock = new Path(dbDir, CrawlDb.LOCK_NAME);
    FileSystem fs = FileSystem.get(getConf());
    LockUtil.createLockFile(fs, lock, force);

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("HostGenerator: starting at " + sdf.format(start));
    LOG.info("HostGenerator: Selecting best-scoring hosts due for resolve.");
    if (topN != Long.MAX_VALUE) {
      LOG.info("HostGenerator: topN: " + topN);
    }
    
    // map to inverted subset due for fetch, sort by score
    JobConf job = new NutchJob(getConf());
    job.setJobName("HostGenerator: select from " + dbDir);

    if (numLists == -1) { // for politeness make
      numLists = job.getNumMapTasks(); // a partition per fetch task
    }
    if ("local".equals(job.get("mapred.job.tracker")) && numLists != 1) {
      // override
      LOG.info("HostGenerator: jobtracker is 'local', generating exactly one partition.");
      numLists = 1;
    }
    job.setLong(GENERATOR_CUR_TIME, curTime);
    // record real generation time
    long generateTime = System.currentTimeMillis();
    job.setLong(Nutch.GENERATE_TIME_KEY, generateTime);
    job.setLong(GENERATOR_TOP_N, topN);

    FileInputFormat.addInputPath(job, new Path(dbDir, CrawlDb.CURRENT_NAME));
    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(Selector.class);
    job.setPartitionerClass(Selector.class);
    job.setReducerClass(Selector.class);

    FileOutputFormat.setOutputPath(job, output);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setMapOutputKeyClass(FloatWritable.class);
    job.setMapOutputValueClass(SelectorEntry.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CrawlDatum.class);
    job.setOutputKeyComparatorClass(DecreasingFloatComparator.class);
    job.setOutputValueClass(CrawlDatum.class);

    try {
      JobClient.runJob(job);
    } catch (IOException e) {
      LockUtil.removeLockFile(fs, lock);
      fs.delete(output, true);
      throw e;
    }


    // TODO: 对主机信息库进行待解析更新，有必要
    if (getConf().getBoolean(GENERATE_UPDATE_CRAWLDB, false)) {
      // update the db from tempDir
      Path tempDir2 = new Path(getConf().get("mapred.temp.dir", ".") + "/generate-temp-"
          + java.util.UUID.randomUUID().toString());

      job = new NutchJob(getConf());
      job.setJobName("generate: updatedb " + dbDir);
      job.setLong(Nutch.GENERATE_TIME_KEY, generateTime);

      FileInputFormat.addInputPath(job, output);

      FileInputFormat.addInputPath(job, new Path(dbDir, CrawlDb.CURRENT_NAME));
      job.setInputFormat(SequenceFileInputFormat.class);
      job.setMapperClass(CrawlDbUpdater.class);
      job.setReducerClass(CrawlDbUpdater.class);
      job.setOutputFormat(MapFileOutputFormat.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(CrawlDatum.class);
      FileOutputFormat.setOutputPath(job, tempDir2);
      try {
        JobClient.runJob(job);
        CrawlDb.install(job, dbDir);
      } catch (IOException e) {
        LockUtil.removeLockFile(fs, lock);
        fs.delete(tempDir2, true);
        throw e;
      }
      fs.delete(tempDir2, true);
    }

    LockUtil.removeLockFile(fs, lock);
//    fs.delete(tempDir, true);

    long end = System.currentTimeMillis();
    LOG.info("HostGenerator: finished at " + sdf.format(end) + ", elapsed: " + TimingUtil.elapsedTime(start, end));

    
    return output;
  }

  private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

  public static synchronized String generateSegmentName() {
    try {
      Thread.sleep(1000);
    } catch (Throwable t) {}
    ;
    return sdf.format(new Date(System.currentTimeMillis()));
  }

  /**
   * Generate a fetchlist from the crawldb.
   */
  public static void main(String args[]) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new HostGenerator(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.out
          .println("Usage: HostGenerator <hostdb> <host_segments_dir> [-force] [-topN N] [-numFetchers numFetchers] [-adddays numDays]");
      return -1;
    }

    Path dbDir = new Path(args[0]);
    Path segmentsDir = new Path(args[1]);
    long curTime = System.currentTimeMillis();
    long topN = Long.MAX_VALUE;
    int numFetchers = -1;
    boolean force = false;

    for (int i = 2; i < args.length; i++) {
      if ("-topN".equals(args[i])) {
        topN = Long.parseLong(args[i + 1]);
        i++;
      } else if ("-numFetchers".equals(args[i])) {
        numFetchers = Integer.parseInt(args[i + 1]);
        i++;
      } else if ("-adddays".equals(args[i])) {
        long numDays = Integer.parseInt(args[i + 1]);
        curTime += numDays * 1000L * 60 * 60 * 24;
      }  else if ("-force".equals(args[i])) {
        force = true;
      } 

    }

    try {
      Path segs = generate(dbDir, segmentsDir, numFetchers, topN, curTime, force);
      if (segs == null) return -1;
    } catch (Exception e) {
      LOG.error("HostGenerator: " + StringUtils.stringifyException(e));
      return -1;
    }
    return 0;
  }
}
