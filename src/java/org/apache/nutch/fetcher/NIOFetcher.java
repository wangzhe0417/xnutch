
package org.apache.nutch.fetcher;

import java.io.File;
import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

// Slf4j Logging imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.util.*;

public class NIOFetcher extends NutchTool implements Tool,
    MapRunnable<Text, CrawlDatum, Text, NutchWritable> {

  public static final Logger LOG = LoggerFactory.getLogger(NIOFetcher.class);

  OutputCollector<Text, NutchWritable> output;
  Reporter reporter;

  String segmentName;
  // TODO: 活动线程没有意义，选择其他的抓取状态跟踪，现在只作为抓取线程是否结束标志
  AtomicInteger activeThreads = new AtomicInteger(0);
  AtomicInteger spinWaiting = new AtomicInteger(0);

  private long start = System.currentTimeMillis(); // start time of fetcher run
  AtomicLong lastRequestStart = new AtomicLong(start);

  // 抓取字节数，只有成功的才计入
  private AtomicLong bytes = new AtomicLong(0); 
  // 成功抓取页面数
  private AtomicInteger pages = new AtomicInteger(0); 
  // 错误页面数，不包括超时的
  AtomicInteger errors = new AtomicInteger(0); 

  AtomicInteger timeouts = new AtomicInteger(0);
  // 活动请求数
  AtomicInteger actives = new AtomicInteger(0);

  boolean storingContent;
  boolean parsing;
  FetchItemQueues fetchQueues;
  QueueFeeder feeder;

  public NIOFetcher() {
    super(null);
  }

  public NIOFetcher(Configuration conf) {
    super(conf);
  }

  private void reportStatus(int pagesLastSec, int bytesLastSec)
      throws IOException {
    StringBuilder status = new StringBuilder();
    Long elapsed = new Long((System.currentTimeMillis() - start) / 1000);

    float avgPagesSec = (float) pages.get() / elapsed.floatValue();
    long avgBytesSec = (bytes.get() / 1024l) / elapsed.longValue();

    status.append(actives).append(" requests (").append(spinWaiting.get())
        .append(" waiting), ");
    status.append(fetchQueues.getQueueCount()).append(" queues, ");
    status.append(fetchQueues.getTotalSize()).append(" URLs queued, ");
    status.append(pages).append(" pages, ").append(errors).append(" errors, ")
        .append(timeouts).append(" timeouts, ");
    status.append(String.format("%.2f", avgPagesSec)).append(" pages/s (");
    status.append(pagesLastSec).append(" last sec), ");
    status.append(avgBytesSec).append(" KB/s (")
        .append((bytesLastSec / 1024)).append(" last sec)");

    LOG.info(status.toString());

    reporter.setStatus(status.toString());
  }

  public void configure(JobConf job) {
    setConf(job);

    this.segmentName = job.get(Nutch.SEGMENT_NAME_KEY);
    this.storingContent = isStoringContent(job);
    this.parsing = isParsing(job);
  }

  public void close() {
  }

  public static boolean isParsing(Configuration conf) {
    return conf.getBoolean("fetcher.parse", true);
  }

  public static boolean isStoringContent(Configuration conf) {
    return conf.getBoolean("fetcher.store.content", true);
  }

  public void run(RecordReader<Text, CrawlDatum> input,
      OutputCollector<Text, NutchWritable> output, Reporter reporter)
      throws IOException {

    this.output = output;
    this.reporter = reporter;
    this.fetchQueues = new FetchItemQueues(getConf());
    
    // 抓取链接供应线程
    int nioFetcherQueueCount = getConf().getInt("http.nio.queue.count",  800);
    feeder = new QueueFeeder(input, fetchQueues, nioFetcherQueueCount);
    // feeder.setPriority((Thread.MAX_PRIORITY + Thread.NORM_PRIORITY) / 2);

    // the value of the time limit is either -1 or the time where it should
    // finish
    long timelimit = getConf().getLong("fetcher.timelimit", -1);
    if (timelimit != -1)
      feeder.setTimeLimit(timelimit);
    feeder.start();

    // 抓取页面队列
    BlockingQueue<Page> pagesQueue = new LinkedBlockingQueue<Page>();

    // 唯一异步抓取线程
    NIOFetcherThread t = new NIOFetcherThread( activeThreads, 
        fetchQueues, feeder, 
        spinWaiting, lastRequestStart, reporter,
        errors, segmentName, 
        pages, bytes, actives,
        timeouts, pagesQueue, getConf());
    t.start();

    // 抓取内容处理线程
    FetchOutputer fetchOutputer = new FetchOutputer(fetchQueues, reporter,
      output, parsing, storingContent, segmentName, getConf());
    PageHandler parser = new PageHandler( pagesQueue, fetchOutputer, getConf());
    Thread thread = new Thread(parser);
    thread.start();

    int timeoutDivisor = getConf().getInt("fetcher.threads.timeout.divisor", 2);
    if (LOG.isInfoEnabled()) {
      LOG.info("NIOFetcher: time-out divisor: " + timeoutDivisor);
    }
    // select a timeout that avoids a task timeout
    long timeout = getConf().getInt("mapred.task.timeout", 10 * 60 * 1000)
        / timeoutDivisor;

    // Used for threshold check, holds pages and bytes processed in the last
    // second
    int pagesLastSec;
    int bytesLastSec;

    // Set to true whenever the threshold has been exceeded for the first time
//    boolean throughputThresholdExceeded = false;
    int throughputThresholdNumRetries = 0;
    int throughputThresholdPages = getConf().getInt(
        "fetcher.throughput.threshold.pages", -1);
    if (LOG.isInfoEnabled()) {
      LOG.info("NIOFetcher: throughput threshold: " + throughputThresholdPages);
    }
    int throughputThresholdMaxRetries = getConf().getInt(
        "fetcher.throughput.threshold.retries", 5);
    if (LOG.isInfoEnabled()) {
      LOG.info("NIOFetcher: throughput threshold retries: "
          + throughputThresholdMaxRetries);
    }
    long throughputThresholdTimeLimit = getConf().getLong(
        "fetcher.throughput.threshold.check.after", -1);

    do {
      // 等待异步抓取完成
      pagesLastSec = pages.get();
      bytesLastSec = (int) bytes.get();

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }

      pagesLastSec = pages.get() - pagesLastSec;
      bytesLastSec = (int) bytes.get() - bytesLastSec;

      reporter.incrCounter("FetcherStatus", "bytes_downloaded", bytesLastSec);

      reportStatus(pagesLastSec, bytesLastSec);

      LOG.info("-activeRequests=" + actives + ", spinWaiting="
          + spinWaiting.get() + ", fetchQueues.totalSize="
          + fetchQueues.getTotalSize() + ", fetchQueues.getQueueCount="
          + fetchQueues.getQueueCount());

      // 已无链接供应，且抓取队列链接数小于5
      if (!feeder.isAlive() && fetchQueues.getTotalSize() < 5) {
        fetchQueues.dump();
      }

      // 检查抓取速度是否过小，若过小，清空抓取队列
      if (throughputThresholdTimeLimit < System.currentTimeMillis()
          && throughputThresholdPages != -1) {
        // Check if we're dropping below the threshold
        if (pagesLastSec < throughputThresholdPages) {
          throughputThresholdNumRetries++;
          LOG.warn(Integer.toString(throughputThresholdNumRetries)
              + ": dropping below configured threshold of "
              + Integer.toString(throughputThresholdPages)
              + " pages per second");

          // Quit if we dropped below threshold too many times
          if (throughputThresholdNumRetries == throughputThresholdMaxRetries) {
            LOG.warn("Dropped below threshold too many times, killing!");

            // Disable the threshold checker
            throughputThresholdPages = -1;

            // Empty the queues cleanly and get number of items that were
            // dropped
            int hitByThrougputThreshold = fetchQueues.emptyQueues();

            if (hitByThrougputThreshold != 0)
              reporter.incrCounter("FetcherStatus", "hitByThrougputThreshold",
                  hitByThrougputThreshold);
          }
        }
      }

      // TODO:带宽控制有必要，但实现思路不一样

      // 检查抓取时限是否已到，若到，清空抓取队列
      if (!feeder.isAlive()) {
        int hitByTimeLimit = fetchQueues.checkTimelimit();
        if (hitByTimeLimit != 0)
          reporter.incrCounter("FetcherStatus", "hitByTimeLimit",
              hitByTimeLimit);
      }

      // some requests seem to hang, despite all intentions
      // TODO: 是否重复进行超时检查
      if ((System.currentTimeMillis() - lastRequestStart.get()) > timeout) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("Aborting with " + actives + " hung requests.");
        }
        return;
      }

    } while (activeThreads.get() > 0);
    LOG.info("-activeRequests=" + actives);

  }

  // 启动异步抓取作业
  public void fetch(Path segment) throws IOException {
    checkConfiguration();

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    if (LOG.isInfoEnabled()) {
      LOG.info("NIOFetcher: starting at " + sdf.format(start));
      LOG.info("NIOFetcher: segment: " + segment);
    }

    // set the actual time for the timelimit relative
    // to the beginning of the whole job and not of a specific task
    // otherwise it keeps trying again if a task fails
    long timelimit = getConf().getLong("fetcher.timelimit.mins", -1);
    if (timelimit != -1) {
      timelimit = System.currentTimeMillis() + (timelimit * 60 * 1000);
      LOG.info("Fetcher Timelimit set for : " + timelimit);
      getConf().setLong("fetcher.timelimit", timelimit);
    }

    // Set the time limit after which the throughput threshold feature is
    // enabled
    timelimit = getConf().getLong("fetcher.throughput.threshold.check.after",
        10);
    timelimit = System.currentTimeMillis() + (timelimit * 60 * 1000);
    getConf().setLong("fetcher.throughput.threshold.check.after", timelimit);

    JobConf job = new NutchJob(getConf());
    job.setJobName("fetch " + segment);

    job.set(Nutch.SEGMENT_NAME_KEY, segment.getName());

    // for politeness, don't permit parallel execution of a single task
    job.setSpeculativeExecution(false);

    FileInputFormat.addInputPath(job, new Path(segment,
        CrawlDatum.GENERATE_DIR_NAME));
    job.setInputFormat(FetcherInputFormat.class);

    job.setMapRunnerClass(NIOFetcher.class);

    FileOutputFormat.setOutputPath(job, segment);
    job.setOutputFormat(FetcherOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NutchWritable.class);

    JobClient.runJob(job);

    long end = System.currentTimeMillis();
    LOG.info("NIOFetcher: finished at " + sdf.format(end) + ", elapsed: "
        + TimingUtil.elapsedTime(start, end));
  }

  /** Run the fetcher. */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new NIOFetcher(),
        args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    String usage = "Usage: NIOFetcher <segment>";

    if (args.length < 1) {
      System.err.println(usage);
      return -1;
    }

    Path segment = new Path(args[0]);

    try {
      fetch(segment);
      return 0;
    } catch (Exception e) {
      LOG.error("Fetcher: " + StringUtils.stringifyException(e));
      return -1;
    }

  }

  // 检查UA是否设置
  private void checkConfiguration() {
    // ensure that a value has been set for the agent name
    String agentName = getConf().get("http.agent.name");
    if (agentName == null || agentName.trim().length() == 0) {
      String message = "Fetcher: No agents listed in 'http.agent.name'"
          + " property.";
      if (LOG.isErrorEnabled()) {
        LOG.error(message);
      }
      throw new IllegalArgumentException(message);
    }
  }

  @Override
  public Map<String, Object> run(Map<String, Object> args, String crawlId)
      throws Exception {

    Map<String, Object> results = new HashMap<String, Object>();
    String RESULT = "result";
    String segment_dir = crawlId + "/segments";
    File segmentsDir = new File(segment_dir);
    File[] segmentsList = segmentsDir.listFiles();
    Arrays.sort(segmentsList, new Comparator<File>() {
      @Override
      public int compare(File f1, File f2) {
        if (f1.lastModified() > f2.lastModified())
          return -1;
        else
          return 0;
      }
    });

    Path segment = new Path(segmentsList[0].getPath());

    try {
      fetch(segment);
      results.put(RESULT, Integer.toString(0));
      return results;
    } catch (Exception e) {
      LOG.error("NIOFetcher: " + StringUtils.stringifyException(e));
      results.put(RESULT, Integer.toString(-1));
      return results;
    }
  }

}
