package org.apache.nutch.crawl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;
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
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.protocol.*;
import org.apache.nutch.util.*;

/**
 * 主机解析。
 * 
 * @author kidden
 *
 */
public class HostFetcher extends Configured implements Tool,
    MapRunnable<Text, CrawlDatum, Text, CrawlDatum> {
  
  public static final Logger LOG = LoggerFactory.getLogger(HostFetcher.class);

  private String[] servers;
  private int timeout;

  private OutputCollector<Text, CrawlDatum> output;
  private RecordReader<Text, CrawlDatum> input;
  private Reporter reporter;

//  private String segmentName;
  private AtomicInteger activeThreads = new AtomicInteger(0);
  private AtomicInteger spinWaiting = new AtomicInteger(0);

  private long start = System.currentTimeMillis(); // start time of fetcher run
  private AtomicLong lastRequestStart = new AtomicLong(start);

//  private AtomicLong bytes = new AtomicLong(0); // total bytes fetched
  private AtomicInteger hosts = new AtomicInteger(0); // total pages fetched
  private AtomicInteger errors = new AtomicInteger(0); // total pages errored


  LinkedList<ResolverThread> fetcherThreads = new LinkedList<ResolverThread>();

  /**
   * This class picks items from queues and fetches the pages.
   */
  private class ResolverThread extends Thread {
    private NameResolver resolver;

    private Configuration conf;
    private String hostInProgress;

    private boolean halted = false;

    public ResolverThread(NameResolver resolver, Configuration conf) {
      this.resolver = resolver;

      this.setDaemon(true); // don't hang JVM on exit
      this.setName("ResolverThread"); // use an informative name
      this.conf = conf;
    }

    @SuppressWarnings("fallthrough")
    public void run() {
      activeThreads.incrementAndGet(); // count threads
      try {

        while (true) {
          // check whether must be stopped
          if (isHalted()) {
            LOG.debug(getName() + " set to halted");
            return;
          }

          Text host = new Text();
          CrawlDatum datum = new CrawlDatum();

          if (!input.next(host, datum)) {
            // all done, finish this thread
            LOG.info("Thread " + getName() + " has no more work available");
            return;
          }

          hostInProgress = host.toString();
          lastRequestStart.set(System.currentTimeMillis());

          // TODO: 失败的都输出RETRY状态，合适吗？
          try {
            LOG.info("Resolving " + host + " with dns server "
                + resolver.dnsServer());
            InetAddress[] ips = resolver.resolve(host.toString());
            if (ips != null && ips.length > 0){
              output(host, datum, ips, ProtocolStatus.STATUS_SUCCESS,
                  CrawlDatum.STATUS_FETCH_SUCCESS);
              hosts.incrementAndGet();
            }
            else
              output(host, datum, null, ProtocolStatus.STATUS_RETRY,
                  CrawlDatum.STATUS_FETCH_RETRY);

          } catch (Throwable t) { // unexpected exception
            logError(host, StringUtils.stringifyException(t));
            output(host, datum, null, ProtocolStatus.STATUS_FAILED,
                CrawlDatum.STATUS_FETCH_RETRY);
          }
        }

      } catch (Throwable e) {
        if (LOG.isErrorEnabled()) {
          LOG.error("fetcher caught:" + e.toString());
        }
      } finally {
        activeThreads.decrementAndGet(); // count threads
        LOG.info("-finishing thread " + getName() + ", activeThreads="
            + activeThreads);
      }
    }

    private void logError(Text url, String message) {
      if (LOG.isInfoEnabled()) {
        LOG.info("fetch of " + url + " failed with: " + message);
      }
      errors.incrementAndGet();
    }

    private void output(Text key, CrawlDatum datum, InetAddress[] ips,
        ProtocolStatus pstatus, int status) {

      datum.setStatus(status);
      datum.setFetchTime(System.currentTimeMillis());
      if (pstatus != null)
        datum.getMetaData().put(Nutch.WRITABLE_PROTO_STATUS_KEY, pstatus);

      datum.setStatus(status);

      if (ips != null)
        IPUtils.setIP(datum, ips);
      
      byte[] signature;
      try {
        signature = IPUtils.calculateSignature(datum);
        datum.setSignature(signature);
      } catch (UnknownHostException e1) {
      }
      

      try {
        output.collect(key, datum);
      } catch (IOException e) {
        if (LOG.isErrorEnabled()) {
          LOG.error("fetcher caught:" + e.toString());
        }
      }

    }

    public synchronized void setHalted(boolean halted) {
      this.halted = halted;
    }

    public synchronized boolean isHalted() {
      return halted;
    }

  }

  public HostFetcher() {
    super(null);
  }

  public HostFetcher(Configuration conf) {
    super(conf);
  }

  private void reportStatus(int hostsLastSec)
      throws IOException {
    StringBuilder status = new StringBuilder();
    Long elapsed = new Long((System.currentTimeMillis() - start) / 1000);

    float avgHostsSec = (float) hosts.get() / elapsed.floatValue();

    status.append(activeThreads).append(" threads (").append(spinWaiting.get())
        .append(" waiting), ");

    status.append(hosts).append(" hosts, ").append(errors).append(" errors, ");
    status.append(String.format("%.2f", avgHostsSec)).append(" hosts/s (");
    status.append(hostsLastSec).append(" last sec), ");

    reporter.setStatus(status.toString());
    
    LOG.info(status.toString());
  }

  public void configure(JobConf job) {
    setConf(job);

//    this.segmentName = job.get(Nutch.SEGMENT_NAME_KEY);

    // if (job.getBoolean("fetcher.verbose", false)) {
    // LOG.setLevel(Level.FINE);
    // }
  }

  public void close() {
  }

  public void run(RecordReader<Text, CrawlDatum> input,
      OutputCollector<Text, CrawlDatum> output, Reporter reporter)
      throws IOException {

    this.output = output;
    this.input = input;
    this.reporter = reporter;

    int timeoutDivisor = getConf().getInt("fetcher.threads.timeout.divisor", 2);
    if (LOG.isInfoEnabled()) {
      LOG.info("Fetcher: time-out divisor: " + timeoutDivisor);
    }


    // TODO: 在各个解析任务间均衡使用域名服务器，避免同时对一个域名服务器发送多个解析请求
    servers = getConf().getStrings("dns.servers");
    timeout = getConf().getInt("dns.timeout", 30);

    // TODO: 解析线程数控制
    if (servers == null) {
      NameResolver resolver = new NameResolver(timeout);
      ResolverThread t = new ResolverThread(resolver, getConf());
      fetcherThreads.add(t);
      t.start();
    } else {
      for (int i = 0; i < servers.length; i++) {
        String s = servers[i];
        NameResolver resolver = new NameResolver(s, timeout);
        ResolverThread t = new ResolverThread(resolver, getConf());
        fetcherThreads.add(t);
        t.start();
      }
    }

    // select a timeout that avoids a task timeout
    long timeout = getConf().getInt("mapred.task.timeout", 10 * 60 * 1000)
        / timeoutDivisor;
    
    // 解析速度检查

    // Used for threshold check, holds pages and bytes processed in the last
    // second
    int hostsLastSec;

    do { // wait for threads to exit
      hostsLastSec = hosts.get();

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }

      hostsLastSec = hosts.get() - hostsLastSec;

      reportStatus(hostsLastSec);
      
      // TODO:吞吐量和限时检查

      // some requests seem to hang, despite all intentions
      if ((System.currentTimeMillis() - lastRequestStart.get()) > timeout) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("Aborting with " + activeThreads + " hung threads.");
          for (int i = 0; i < fetcherThreads.size(); i++) {
            ResolverThread thread = fetcherThreads.get(i);
            if (thread.isAlive()) {
              LOG.warn("Thread #" + i + " hung while processing "
                  + thread.hostInProgress);
              if (LOG.isDebugEnabled()) {
                StackTraceElement[] stack = thread.getStackTrace();
                StringBuilder sb = new StringBuilder();
                sb.append("Stack of thread #").append(i).append(":\n");
                for (StackTraceElement s : stack) {
                  sb.append(s.toString()).append('\n');
                }
                LOG.debug(sb.toString());
              }
            }
          }
        }
        return;
      }

    } while (activeThreads.get() > 0);
    LOG.info("-activeThreads=" + activeThreads);

  }

  public void fetch(Path segment, int threads) throws IOException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    if (LOG.isInfoEnabled()) {
      LOG.info("HostFetcher: starting at " + sdf.format(start));
      LOG.info("HostFetcher: segment: " + segment);
    }

    // set the actual time for the timelimit relative
    // to the beginning of the whole job and not of a specific task
    // otherwise it keeps trying again if a task fails
    long timelimit = getConf().getLong("fetcher.timelimit.mins", -1);
    if (timelimit != -1) {
      timelimit = System.currentTimeMillis() + (timelimit * 60 * 1000);
      LOG.info("HostFetcher Timelimit set for : " + timelimit);
      getConf().setLong("fetcher.timelimit", timelimit);
    }

    // Set the time limit after which the throughput threshold feature is
    // enabled
    timelimit = getConf().getLong("fetcher.throughput.threshold.check.after",
        10);
    timelimit = System.currentTimeMillis() + (timelimit * 60 * 1000);
    getConf().setLong("fetcher.throughput.threshold.check.after", timelimit);

    JobConf job = new NutchJob(getConf());
    job.setJobName("host fetch " + segment);

    job.setInt("fetcher.threads.fetch", threads);
    job.set(Nutch.SEGMENT_NAME_KEY, segment.getName());

    // for politeness, don't permit parallel execution of a single task
    job.setSpeculativeExecution(false);

    FileInputFormat.addInputPath(job, new Path(segment,
        CrawlDatum.GENERATE_DIR_NAME));
    job.setInputFormat(SequenceFileInputFormat.class);

    FileOutputFormat.setOutputPath(job, new Path(segment,
        CrawlDatum.FETCH_DIR_NAME));

    job.setMapRunnerClass(HostFetcher.class);

    // FileOutputFormat.setOutputPath(job, segment);
    job.setOutputFormat(MapFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CrawlDatum.class);

    JobClient.runJob(job);

    long end = System.currentTimeMillis();
    LOG.info("HostFetcher: finished at " + sdf.format(end) + ", elapsed: "
        + TimingUtil.elapsedTime(start, end));
  }

  /** Run the fetcher. */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new HostFetcher(),
        args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {

    String usage = "Usage: HostFetcher <segment>";

    if (args.length < 1) {
      System.err.println(usage);
      return -1;
    }

    Path segment = new Path(args[0]);

    int threads = getConf().getInt("fetcher.threads.fetch", 10);

    getConf().setInt("fetcher.threads.fetch", threads);

    try {
      fetch(segment, threads);
      return 0;
    } catch (Exception e) {
      LOG.error("HostFetcher: " + StringUtils.stringifyException(e));
      return -1;
    }

  }

}
