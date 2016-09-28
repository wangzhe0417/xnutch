package org.apache.nutch.crawl;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Closeable;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.TreeMap;

// Commons Logging imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Progressable;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.StringUtil;

/**
 * HostDB读取工具
 *
 * 转储（各种条件下）和统计
 */
public class HostDbReader implements Closeable {

  public static final Logger LOG = LoggerFactory.getLogger(HostDbReader.class);

  private MapFile.Reader[] readers = null;

  private void openReaders(String hostDb, Configuration config) throws IOException {
    if (readers != null) return;
    FileSystem fs = FileSystem.get(config);
    readers = MapFileOutputFormat.getReaders(fs, new Path(hostDb,
        CrawlDb.CURRENT_NAME), config);
  }

  private void closeReaders() {
    if (readers == null) return;
    for (int i = 0; i < readers.length; i++) {
      try {
        readers[i].close();
      } catch (Exception e) {

      }
    }
  }

  public static class CrawlDatumCsvOutputFormat extends FileOutputFormat<Text,CrawlDatum> {
    protected static class LineRecordWriter implements RecordWriter<Text,CrawlDatum> {
      private DataOutputStream out;
      public LineRecordWriter(DataOutputStream out) {
        this.out = out;
        try {
          out.writeBytes("Host;Status code;Status name;Fetch Time;Modified Time;Retries since fetch;Retry interval seconds;Retry interval days;Score;Signature;Metadata\n");
        } catch (IOException e) {}
      }

      public synchronized void write(Text key, CrawlDatum value) throws IOException {
          out.writeByte('"');
          out.writeBytes(key.toString());
          out.writeByte('"');
          out.writeByte(';');
          out.writeBytes(Integer.toString(value.getStatus()));
          out.writeByte(';');
          out.writeByte('"');
          out.writeBytes(CrawlDatum.getStatusName(value.getStatus()));
          out.writeByte('"');
          out.writeByte(';');
          out.writeBytes(new Date(value.getFetchTime()).toString());
          out.writeByte(';');
          out.writeBytes(new Date(value.getModifiedTime()).toString());
          out.writeByte(';');
          out.writeBytes(Integer.toString(value.getRetriesSinceFetch()));
          out.writeByte(';');
          out.writeBytes(Float.toString(value.getFetchInterval()));
          out.writeByte(';');
          out.writeBytes(Float.toString((value.getFetchInterval() / FetchSchedule.SECONDS_PER_DAY)));
          out.writeByte(';');
          out.writeBytes(Float.toString(value.getScore()));
          out.writeByte(';');
          out.writeByte('"');
          out.writeBytes(value.getSignature() != null ? StringUtil.toHexString(value.getSignature()): "null");
          out.writeByte('"');
          out.writeByte(';');
          out.writeByte('"');
          if (value.getMetaData() != null) {
            for (Entry<Writable, Writable> e : value.getMetaData().entrySet()) {
              out.writeBytes(e.getKey().toString());
              out.writeByte(':');
              out.writeBytes(e.getValue().toString());
              out.writeBytes("|||");
            }
          }
          out.writeByte('"');

          out.writeByte('\n');
      }

      public synchronized void close(Reporter reporter) throws IOException {
        out.close();
      }
    }

    public RecordWriter<Text,CrawlDatum> getRecordWriter(FileSystem fs, JobConf job, String name,
        Progressable progress) throws IOException {
      Path dir = FileOutputFormat.getOutputPath(job);
      DataOutputStream fileOut = fs.create(new Path(dir, name), progress);
      return new LineRecordWriter(fileOut);
   }
  }

  public static class CrawlDbStatMapper implements Mapper<Text, CrawlDatum, Text, LongWritable> {
    LongWritable COUNT_1 = new LongWritable(1);
    private boolean sort = false;
    public void configure(JobConf job) {
      sort = job.getBoolean("db.reader.stats.sort", false );
    }
    public void close() {}
    public void map(Text key, CrawlDatum value, OutputCollector<Text, LongWritable> output, Reporter reporter)
            throws IOException {
      output.collect(new Text("T"), COUNT_1);
      output.collect(new Text("status " + value.getStatus()), COUNT_1);
      output.collect(new Text("retry " + value.getRetriesSinceFetch()), COUNT_1);
      output.collect(new Text("s"), new LongWritable((long) (value.getScore() * 1000.0)));
      if(sort){
        //URL u = new URL(key.toString());
        String host = key.toString();//u.getHost();
        output.collect(new Text("status " + value.getStatus() + " " + host), COUNT_1);
      }
    }
  }

  public static class CrawlDbStatCombiner implements Reducer<Text, LongWritable, Text, LongWritable> {
    LongWritable val = new LongWritable();

    public CrawlDbStatCombiner() { }
    public void configure(JobConf job) { }
    public void close() {}
    public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, LongWritable> output, Reporter reporter)
        throws IOException {
      val.set(0L);
      String k = key.toString();
      if (!k.equals("s")) {
        while (values.hasNext()) {
          LongWritable cnt = values.next();
          val.set(val.get() + cnt.get());
        }
        output.collect(key, val);
      } else {
        long total = 0;
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        while (values.hasNext()) {
          LongWritable cnt = values.next();
          if (cnt.get() < min) min = cnt.get();
          if (cnt.get() > max) max = cnt.get();
          total += cnt.get();
        }
        output.collect(new Text("scn"), new LongWritable(min));
        output.collect(new Text("scx"), new LongWritable(max));
        output.collect(new Text("sct"), new LongWritable(total));
      }
    }
  }

  public static class CrawlDbStatReducer implements Reducer<Text, LongWritable, Text, LongWritable> {
    public void configure(JobConf job) {}
    public void close() {}
    public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, LongWritable> output, Reporter reporter)
            throws IOException {

      String k = key.toString();
      if (k.equals("T")) {
        // sum all values for this key
        long sum = 0;
        while (values.hasNext()) {
          sum += values.next().get();
        }
        // output sum
        output.collect(key, new LongWritable(sum));
      } else if (k.startsWith("status") || k.startsWith("retry")) {
        LongWritable cnt = new LongWritable();
        while (values.hasNext()) {
          LongWritable val = values.next();
          cnt.set(cnt.get() + val.get());
        }
        output.collect(key, cnt);
      } else if (k.equals("scx")) {
        LongWritable cnt = new LongWritable(Long.MIN_VALUE);
        while (values.hasNext()) {
          LongWritable val = values.next();
          if (cnt.get() < val.get()) cnt.set(val.get());
        }
        output.collect(key, cnt);
      } else if (k.equals("scn")) {
        LongWritable cnt = new LongWritable(Long.MAX_VALUE);
        while (values.hasNext()) {
          LongWritable val = values.next();
          if (cnt.get() > val.get()) cnt.set(val.get());
        }
        output.collect(key, cnt);
      } else if (k.equals("sct")) {
        LongWritable cnt = new LongWritable();
        while (values.hasNext()) {
          LongWritable val = values.next();
          cnt.set(cnt.get() + val.get());
        }
        output.collect(key, cnt);
      }
    }
  }

  public static class CrawlDbTopNMapper implements Mapper<Text, CrawlDatum, FloatWritable, Text> {
    private static final FloatWritable fw = new FloatWritable();
    private float min = 0.0f;

    public void configure(JobConf job) {
      long lmin = job.getLong("db.reader.topn.min", 0);
      if (lmin != 0) {
        min = (float)lmin / 1000000.0f;
      }
    }
    public void close() {}
    public void map(Text key, CrawlDatum value, OutputCollector<FloatWritable, Text> output, Reporter reporter)
            throws IOException {
      if (value.getScore() < min) return; // don't collect low-scoring records
      fw.set(-value.getScore()); // reverse sorting order
      output.collect(fw, key); // invert mapping: score -> url
    }
  }

  public static class CrawlDbTopNReducer implements Reducer<FloatWritable, Text, FloatWritable, Text> {
    private long topN;
    private long count = 0L;

    public void reduce(FloatWritable key, Iterator<Text> values, OutputCollector<FloatWritable, Text> output, Reporter reporter) throws IOException {
      while (values.hasNext() && count < topN) {
        key.set(-key.get());
        output.collect(key, values.next());
        count++;
      }
    }

    public void configure(JobConf job) {
      topN = job.getLong("db.reader.topn", 100) / job.getNumReduceTasks();
    }

    public void close() {}
  }

  public void close() {
    closeReaders();
  }

  public void processStatJob(String hostDb, Configuration config, boolean sort) throws IOException {

    if (LOG.isInfoEnabled()) {
      LOG.info("HostDb statistics start: " + hostDb);
    }

    Path tmpFolder = new Path(hostDb, "stat_tmp" + System.currentTimeMillis());

    JobConf job = new NutchJob(config);
    job.setJobName("stats " + hostDb);
    job.setBoolean("db.reader.stats.sort", sort);

    FileInputFormat.addInputPath(job, new Path(hostDb, CrawlDb.CURRENT_NAME));
    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(CrawlDbStatMapper.class);
    job.setCombinerClass(CrawlDbStatCombiner.class);
    job.setReducerClass(CrawlDbStatReducer.class);

    FileOutputFormat.setOutputPath(job, tmpFolder);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    // https://issues.apache.org/jira/browse/NUTCH-1029
    job.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);

    JobClient.runJob(job);

    // reading the result
    FileSystem fileSystem = FileSystem.get(config);
    SequenceFile.Reader[] readers = SequenceFileOutputFormat.getReaders(config, tmpFolder);

    Text key = new Text();
    LongWritable value = new LongWritable();

    TreeMap<String, LongWritable> stats = new TreeMap<String, LongWritable>();
    for (int i = 0; i < readers.length; i++) {
      SequenceFile.Reader reader = readers[i];
      while (reader.next(key, value)) {
        String k = key.toString();
        LongWritable val = stats.get(k);
        if (val == null) {
          val = new LongWritable();
          if (k.equals("scx")) val.set(Long.MIN_VALUE);
          if (k.equals("scn")) val.set(Long.MAX_VALUE);
          stats.put(k, val);
        }
        if (k.equals("scx")) {
          if (val.get() < value.get()) val.set(value.get());
        } else if (k.equals("scn")) {
          if (val.get() > value.get()) val.set(value.get());
        } else {
          val.set(val.get() + value.get());
        }
      }
      reader.close();
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("Statistics for HostDb: " + hostDb);
      LongWritable totalCnt = stats.get("T");
      stats.remove("T");
      LOG.info("TOTAL hosts:\t" + totalCnt.get());
      for (Map.Entry<String, LongWritable> entry : stats.entrySet()) {
        String k = entry.getKey();
        LongWritable val = entry.getValue();
        if (k.equals("scn")) {
          LOG.info("min score:\t" + (val.get() / 1000.0f));
        } else if (k.equals("scx")) {
          LOG.info("max score:\t" + (val.get() / 1000.0f));
        } else if (k.equals("sct")) {
          LOG.info("avg score:\t" + (float) ((((double)val.get()) / totalCnt.get()) / 1000.0));
        } else if (k.startsWith("status")) {
          String[] st = k.split(" ");
          int code = Integer.parseInt(st[1]);
          if(st.length >2 ) LOG.info("   " + st[2] +" :\t" + val);
          else LOG.info(st[0] +" " +code + " (" + CrawlDatum.getStatusName((byte) code) + "):\t" + val);
        } else LOG.info(k + ":\t" + val);
      }
    }
    // removing the tmp folder
    fileSystem.delete(tmpFolder, true);
    if (LOG.isInfoEnabled()) { LOG.info("HostDb statistics: done"); }

  }

  public CrawlDatum get(String hostDb, String host, Configuration config) throws IOException {
    Text key = new Text(host);
    CrawlDatum val = new CrawlDatum();
    openReaders(hostDb, config);
    CrawlDatum res = (CrawlDatum)MapFileOutputFormat.getEntry(readers,
        new HashPartitioner<Text, CrawlDatum>(), key, val);
    return res;
  }

  public void readUrl(String hostDb, String host, Configuration config) throws IOException {
    CrawlDatum res = get(hostDb, host, config);
    System.out.println("HOST: " + host);
    if (res != null) {
      System.out.println(res);
    } else {
      System.out.println("not found");
    }
  }

  public void processDumpJob(String crawlDb, String output, Configuration config, String format, String regex, String status, Integer retry) throws IOException {
    if (LOG.isInfoEnabled()) {
      LOG.info("HostDb dump: starting");
      LOG.info("HostDb db: " + crawlDb);
    }

    Path outFolder = new Path(output);

    JobConf job = new NutchJob(config);
    job.setJobName("dump " + crawlDb);

    FileInputFormat.addInputPath(job, new Path(crawlDb, CrawlDb.CURRENT_NAME));
    job.setInputFormat(SequenceFileInputFormat.class);
    FileOutputFormat.setOutputPath(job, outFolder);

    if (format.equals("csv")) {
      job.setOutputFormat(CrawlDatumCsvOutputFormat.class);
    }
    else if (format.equals("hostdb")) {
      job.setOutputFormat(MapFileOutputFormat.class);
    } else {
      job.setOutputFormat(TextOutputFormat.class);
    }

    if (status != null) job.set("status", status);
    if (regex != null) job.set("regex", regex);
    if (retry != null) job.setInt("retry", retry);
    
    job.setMapperClass(CrawlDbDumpMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CrawlDatum.class);

    JobClient.runJob(job);
    if (LOG.isInfoEnabled()) { LOG.info("HostDb dump: done"); }
  }

  public static class CrawlDbDumpMapper implements Mapper<Text, CrawlDatum, Text, CrawlDatum> {
    Pattern pattern = null;
    Matcher matcher = null;
    String status = null;
    Integer retry = null;

    public void configure(JobConf job) {
      if (job.get("regex", null) != null) {
        pattern = Pattern.compile(job.get("regex"));
      }
      status = job.get("status", null);
      retry = job.getInt("retry", -1);
    }

    public void close() {}
    public void map(Text key, CrawlDatum value, OutputCollector<Text, CrawlDatum> output, Reporter reporter)
            throws IOException {
            
      // check retry
      if (retry != -1) {
        if (value.getRetriesSinceFetch() < retry) {
          return;
        }
      }

      // check status
      if (status != null
        && !status.equalsIgnoreCase(CrawlDatum.getStatusName(value.getStatus()))) return;

      // check regex
      if (pattern != null) {
        matcher = pattern.matcher(key.toString());
        if (!matcher.matches()) {
          return;
        }
      }

      output.collect(key, value);
    }
  }

  public void processTopNJob(String crawlDb, long topN, float min, String output, Configuration config) throws IOException {

    if (LOG.isInfoEnabled()) {
      LOG.info("HostDb topN: starting (topN=" + topN + ", min=" + min + ")");
      LOG.info("HostDb db: " + crawlDb);
    }

    Path outFolder = new Path(output);
    Path tempDir =
      new Path(config.get("mapred.temp.dir", ".") +
               "/readdb-topN-temp-"+
               Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    JobConf job = new NutchJob(config);
    job.setJobName("topN prepare " + crawlDb);
    FileInputFormat.addInputPath(job, new Path(crawlDb, CrawlDb.CURRENT_NAME));
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapperClass(CrawlDbTopNMapper.class);
    job.setReducerClass(IdentityReducer.class);

    FileOutputFormat.setOutputPath(job, tempDir);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(FloatWritable.class);
    job.setOutputValueClass(Text.class);

    // XXX hmmm, no setFloat() in the API ... :(
    job.setLong("db.reader.topn.min", Math.round(1000000.0 * min));
    JobClient.runJob(job);

    if (LOG.isInfoEnabled()) {
      LOG.info("CrawlDb topN: collecting topN scores.");
    }
    job = new NutchJob(config);
    job.setJobName("topN collect " + crawlDb);
    job.setLong("db.reader.topn", topN);

    FileInputFormat.addInputPath(job, tempDir);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapperClass(IdentityMapper.class);
    job.setReducerClass(CrawlDbTopNReducer.class);

    FileOutputFormat.setOutputPath(job, outFolder);
    job.setOutputFormat(TextOutputFormat.class);
    job.setOutputKeyClass(FloatWritable.class);
    job.setOutputValueClass(Text.class);

    job.setNumReduceTasks(1); // create a single file.

    JobClient.runJob(job);
    FileSystem fs = FileSystem.get(config);
    fs.delete(tempDir, true);
    if (LOG.isInfoEnabled()) { LOG.info("CrawlDb topN: done"); }

  }

  public static void main(String[] args) throws IOException {
    HostDbReader dbr = new HostDbReader();

    if (args.length < 2) {
      System.err.println("Usage: HostDbReader <hostdb> (-stats | -dump <out_dir> | -topN <nnnn> <out_dir> [<min>] | -host <host>)");
      System.err.println("\t<hostdb>\tdirectory name where hostdb is located");
      System.err.println("\t-stats [-sort] \tprint overall statistics to System.out");
      System.err.println("\t\t[-sort]\tlist status sorted by host");
      System.err.println("\t-dump <out_dir> [-format normal|csv|hostdb]\tdump the whole db to a text file in <out_dir>");
      System.err.println("\t\t[-format csv]\tdump in Csv format");
      System.err.println("\t\t[-format normal]\tdump in standard format (default option)");
      System.err.println("\t\t[-format hostdb]\tdump as HostDB");
      System.err.println("\t\t[-regex <expr>]\tfilter records with expression");
      System.err.println("\t\t[-retry <num>]\tminimum retry count");
      System.err.println("\t\t[-status <status>]\tfilter records by CrawlDatum status");
      System.err.println("\t-host <host>\tprint information on <host> to System.out");
      System.err.println("\t-topN <nnnn> <out_dir> [<min>]\tdump top <nnnn> urls sorted by score to <out_dir>");
      System.err.println("\t\t[<min>]\tskip records with scores below this value.");
      System.err.println("\t\t\tThis can significantly improve performance.");
      return;
    }
    String param = null;
    String hostDb = args[0];
    Configuration conf = NutchConfiguration.create();
    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-stats")) {
        boolean toSort = false;
        if(i < args.length - 1 && "-sort".equals(args[i+1])){
          toSort = true;
          i++;
        }
        dbr.processStatJob(hostDb, conf, toSort);
      } else if (args[i].equals("-dump")) {
        param = args[++i];
        String format = "normal";
        String regex = null;
        Integer retry = null;
        String status = null;
        for (int j = i + 1; j < args.length; j++) {
          if (args[j].equals("-format")) {
            format = args[++j];
            i=i+2;
          }
          if (args[j].equals("-regex")) {
            regex = args[++j];
            i=i+2;
          }
          if (args[j].equals("-retry")) {
            retry = Integer.parseInt(args[++j]);
            i=i+2;
          }
          if (args[j].equals("-status")) {
            status = args[++j];
            i=i+2;
          }
        }
        dbr.processDumpJob(hostDb, param, conf, format, regex, status, retry);
      } else if (args[i].equals("-host")) {
        param = args[++i];
        dbr.readUrl(hostDb, param, conf);
      } else if (args[i].equals("-topN")) {
        param = args[++i];
        long topN = Long.parseLong(param);
        param = args[++i];
        float min = 0.0f;
        if (i < args.length - 1) {
          min = Float.parseFloat(args[++i]);
        }
        dbr.processTopNJob(hostDb, topN, min, param, conf);
      } else {
        System.err.println("\nError: wrong argument " + args[i]);
      }
    }
    return;
  }
}
