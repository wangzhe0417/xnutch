package org.apache.nutch.fetcher;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import crawlercommons.robots.BaseRobotRules;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.protocol.ProtocolStatus;
import org.apache.nutch.util.IPUtils;
import org.apache.nutch.util.RobotsUtils;

class NIOFetcherThread extends Thread {
  
  static class CrawlState {
    public int state;
    public long lastOpTime;
    public SelectionKey key;

    // 通道状态
    public final static byte CONNECTING = 0;
    public final static byte CONNECTED = 1;
    public final static byte REQUESTED = 2;
    public final static byte READING = 3;
  }
  
    private Configuration conf;
    
    private AtomicInteger activeThreads;

    private FetchItemQueues fetchQueues;

    private QueueFeeder feeder;

    private AtomicInteger spinWaiting;

    private AtomicLong lastRequestStart;

    private AtomicInteger errors;
    
    private AtomicInteger pages;

    private AtomicLong bytes;
    
    AtomicInteger actives;
    
    AtomicInteger timeouts;

    // IO选择器
    private Selector selector;

    private ByteBuffer readBuffer = ByteBuffer.allocate(8192);
    
    // 发送到服务器的HTTP请求 链接 -> 请求
    private Map<String, ByteBuffer> writeBuffers = new HashMap<String, ByteBuffer>();
    
    // 服务器响应内容，链接->内容
    private Map<String, ByteArrayOutputStream> streams = new HashMap<String, ByteArrayOutputStream>();

    private HttpRequestBuilder httpRequestBuilder = new HttpRequestBuilder();

    // select操作等待，毫秒，<=0,立即返回
    private int selectTimeout = 2000;

    // 处理中的异步请求数
    // / XXX: 和actives有何区别？
    private int inProgress = 0;

    // 最大处理异步请求数
    private int maxInProgress = 200;

    // 抓取队列中是否还有链接
    private boolean hasUrls = true;

    // TODO: 区分连接超时和读取超时
    // 异步请求超时
    private int timeout;

    // 异步请求超时检查间隔，毫秒
    private int timeoutCheckInterval = 3000;

    // 上次异步请求超时检查时间
    private long lastTimeoutCheck = 0;

    // 异步请求状态，链接 -> 状态
    private HashMap<String, CrawlState> states = new HashMap<String, CrawlState>();

    // 待处理的抓取内容
    private BlockingQueue<Page> pagesQueue;

    private String agent;
    private BaseRobotRules rules;
    private boolean checkRobots;
    private long maxCrawlDelay;

    public NIOFetcherThread( AtomicInteger activeThreads, 
        FetchItemQueues fetchQueues, 
        QueueFeeder feeder, 
        AtomicInteger spinWaiting, 
        AtomicLong lastRequestStart, 
        Reporter reporter,
        AtomicInteger errors, 
        String segmentName, 
        AtomicInteger pages, 
        AtomicLong bytes, 
        AtomicInteger actives,
        AtomicInteger timeouts,
        BlockingQueue<Page> pagesQueue,
        Configuration conf)
        throws IOException {
      
      this.activeThreads = activeThreads;
      this.fetchQueues = fetchQueues;
      this.feeder = feeder;
      this.spinWaiting = spinWaiting;
      this.lastRequestStart = lastRequestStart;
      this.errors = errors;
      this.pages = pages;
      this.bytes = bytes;
      this.actives = actives;
      this.timeouts = timeouts;

      this.setDaemon(true); // don't hang JVM on exit
      this.setName("FetcherThread"); // use an informative name

      this.pagesQueue = pagesQueue;

      this.conf = conf;

      selectTimeout = conf.getInt("http.nio.timeout.select", 1000);
      timeout = conf.getInt("http.nio.timeout", 30000);
      maxInProgress = conf.getInt("http.nio.request.max", 200);
      timeoutCheckInterval = conf.getInt("http.nio.timeout.interval", 4000);

      selector = Selector.open();

      agent = conf.get("http.agent.name");
      checkRobots = conf.getBoolean("fetcher.robots.check", true);
      this.maxCrawlDelay = conf.getInt("fetcher.max.crawl.delay", 30) * 1000;
    }

    public void fetch() {
      while (true) {
        initiateNewConnections();

        // 选择就绪事件
        int nb;
        try {
          if (selectTimeout <= 0)
            nb = selector.selectNow();
          else
            nb = selector.select(selectTimeout);
        } catch (IOException e) {
          NIOFetcher.LOG.warn("nio select: " + e.toString());
          continue;
        }

        // if (nb == 0)
        // 超时检查
        if (System.currentTimeMillis() - lastTimeoutCheck >= timeoutCheckInterval) {
          lastTimeoutCheck = System.currentTimeMillis();
          checkTimeout();
        }

        if (inProgress == 0 && !hasUrls) // 无处理中的请求和无可抓取的链接
          break;

        // 对所有就绪事件进行依次处理
        Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
        while (iter.hasNext()) {
          SelectionKey key = iter.next();
          iter.remove();

          // if (!key.isValid()) {
          // continue;
          // }

          try {
            if (key.isConnectable()) { // 可以连接
              connect(key);
            } else if (key.isWritable()) { // 可以发送HTTP请求
              write(key);
            } else if (key.isReadable()) { // 可以读取服务器数据
              read(key);
            }
          } catch (Exception e) {
            FetchItem att = (FetchItem) key.attachment();
            String url = att.getUrl().toString();
            // LOG.warn(url + ": " + e);
            logError(new Text(url), e.toString());
            
            int killedURLs = fetchQueues
                .checkExceptionThreshold(att.getQueueID());
//            if (killedURLs != 0)
//              reporter.incrCounter("FetcherStatus",
//                  "AboveExceptionThresholdInQueue", killedURLs);

            finishChannel(key, CrawlDatum.STATUS_FETCH_RETRY, null);
          }

        }

        // checkTimeout();

      }

      NIOFetcher.LOG.info("退出抓取循环");
      pagesQueue.add(Page.EndPage);
    }

    /**
     * 检查和中止超时的异步请求
     */
    private void checkTimeout() {
      NIOFetcher.LOG.info("超时检查...");
      HashMap<String, CrawlState> copyStates = new HashMap<String, CrawlState>();
      for (Map.Entry<String, CrawlState> e : states.entrySet()) {
        copyStates.put(e.getKey(), e.getValue());
      }

      long current = System.currentTimeMillis();

      for (Map.Entry<String, CrawlState> e : copyStates.entrySet()) {
        CrawlState s = e.getValue();
        if (current - s.lastOpTime > timeout) {
          // LOG.warn(e.getKey() + ": time out");
          this.timeouts.incrementAndGet();
          finishChannel(s.key, CrawlDatum.STATUS_FETCH_RETRY, null);
        }
        // if (s.State == 0 && current - s.lastOp > connectTimeout)
        // {
        // LOG.warn(e.getKey() + ": connect time out");
        // finishChannel(s.key, CrawlDatum.STATUS_FETCH_RETRY, null);
        // errConnect++;
        // }
        // else if (s.State == 2 && current - s.lastOp > readTimeout)
        // {
        // LOG.warn(e.getKey() + ": read time out");
        //
        // finishChannel(s.key, CrawlDatum.STATUS_FETCH_RETRY, null);
        // errRead++;
        // }
      }

      copyStates.clear();
    }

    /**
     * 发送异步连接请求
     */
    private void initiateNewConnections() {
      while (inProgress < maxInProgress && hasUrls) {
        FetchItem fit = this.fetchQueues.getFetchItem();
        if (fit == null) {
          if (this.feeder.isAlive() || this.fetchQueues.getTotalSize() > 0) { // 抓取队列有链接，但礼貌性原因无暂无可抓链接
            NIOFetcher.LOG.debug(getName() + " spin-waiting ...");
            this.spinWaiting.incrementAndGet();
          } else { // 无可抓链接
            hasUrls = false;
            NIOFetcher.LOG.info("Thread " + getName() + " has no more work available");
          }
          
          return;
        }

        this.lastRequestStart.set(System.currentTimeMillis());

        SocketChannel socketChannel = null;
        SelectionKey key = null;
        try {
          //对缓存robots.txt进行支持
          if (checkRobots) {
            if (RobotsUtils.hasRobots(fit.datum)) {
              NIOFetcher.LOG.info("Cached robots.txt: " + fit.u.toString());
              rules = RobotsUtils.getRobots(fit.url.toString(), fit.datum, agent);

              if (!rules.isAllowed(fit.url.toString())) {
                this.fetchQueues.finishFetchItem(fit);
                if (NIOFetcher.LOG.isDebugEnabled()) {
                  NIOFetcher.LOG.info("Denied by robots.txt: " + fit.url);
                }
                fit.datum.getMetaData().put(Nutch.WRITABLE_PROTO_STATUS_KEY,
                    ProtocolStatus.STATUS_ROBOTS_DENIED);
                Page robotsDeniedPage = new Page(fit.getUrl().toString(), fit.getDatum());
                pagesQueue.add(robotsDeniedPage);
                continue;
              }

              if (rules.getCrawlDelay() > 0 ) {
                if (rules.getCrawlDelay() > maxCrawlDelay && maxCrawlDelay >= 0) {
                  this.fetchQueues.finishFetchItem(fit);
                  NIOFetcher.LOG.info("Crawl-Delay for " + fit.url + " too long ("
                      + rules.getCrawlDelay() + "), skipping");
                  fit.datum.getMetaData().put(Nutch.WRITABLE_PROTO_STATUS_KEY,
                      ProtocolStatus.STATUS_ROBOTS_DENIED);
                  Page robotsDeniedPage = new Page(fit.getUrl().toString(), fit.getDatum());
                  pagesQueue.add(robotsDeniedPage);
                  continue;
                } else {
                  FetchItemQueue fiq = this.fetchQueues.getFetchItemQueue(fit.queueID);
                  fiq.crawlDelay = rules.getCrawlDelay();
                  if (NIOFetcher.LOG.isDebugEnabled()) {
                    NIOFetcher.LOG.info(
                        "Crawl delay for queue: " + fit.queueID + " is set to " + fiq.crawlDelay
                        + " as per robots.txt. url: " + fit.url);
                  }
                }
              }
            }
          }

          int port = fit.u.getPort();
          port = port > 0 ? port : 80;
          String ipStr = IPUtils.getIPString(fit.datum);

          // LOG.info("Connecting " + url.toString() + "...");

          // TODO: 没有进行robots.txt检查

          InetAddress addr = null;

          //没有缓存IP时,使用系统默认的方式解析
          if (ipStr.length() == 0)  {
            addr = InetAddress.getByName(fit.u.toString());
            NIOFetcher.LOG.info("Connecting " + fit.u.toString() + " by resolving host...");
          } else {
            addr = IPUtils.toIP(ipStr);
          }

          InetSocketAddress ia = new InetSocketAddress(addr, port);
          // if (ia.isUnresolved())
          // continue;

          // LOG.info("Fetching " + fit.url);

          socketChannel = SocketChannel.open();
          socketChannel.configureBlocking(false);
          socketChannel.connect(ia);
          key = socketChannel.register(selector, SelectionKey.OP_CONNECT);

          key.attach(fit);
          streams.put(fit.url.toString(), new ByteArrayOutputStream());

          CrawlState s = new CrawlState();
          s.key = key;
          s.state = CrawlState.CONNECTING;
          s.lastOpTime = System.currentTimeMillis();
          states.put(fit.url.toString(), s);

          // XXX: 这两个量是否可合并
          inProgress++;
          this.actives.incrementAndGet();
        } catch (IOException e) {
          // TODO: 异常处理结构放在哪里更合适？
          this.fetchQueues.finishFetchItem(fit);
          
          logError(fit.url, e.toString());
          
          if (key != null)
            key.cancel();
          
          if (socketChannel != null) {
            try {
              socketChannel.close();
            } catch (IOException e1) {
              NIOFetcher.LOG.info(fit.u.toString() + ": " + e.toString());
            }
          }
        }
      }
    }

    /**
     * 完成异步连接
     * 
     * @param key
     *          选择键
     * @throws IOException
     */
    private void connect(SelectionKey key) throws IOException {
      SocketChannel socketChannel = (SocketChannel) key.channel();
      FetchItem att = (FetchItem) key.attachment();
      String url = att.getUrl().toString();

      socketChannel.finishConnect();
      // LOG.info(url + ": connected");

      key.interestOps(SelectionKey.OP_WRITE);

      updateState(url);
    }

    /**
     * 更新连接的异步处理状态
     * 
     * @param url
     *          链接
     */
    private void updateState(String url) {
      // TODO: 抓取状态未更新
      CrawlState stat = states.get(url);
      stat.lastOpTime = System.currentTimeMillis();
    }

    /**
     * 发送HTTP请求头到服务器
     * 
     * @param key
     *          选择键
     * @throws IOException
     */
    private void write(SelectionKey key) throws IOException {
      FetchItem att = (FetchItem) key.attachment();
      String url = att.getUrl().toString();
      SocketChannel socketChannel = (SocketChannel) key.channel();

      ByteBuffer writeBuffer = writeBuffers.get(url);
      if (writeBuffer == null) {
        String getRequest = httpRequestBuilder.buildGet(url);
        writeBuffer = ByteBuffer.wrap(getRequest.getBytes());
        writeBuffers.put(url, writeBuffer);
      }

      socketChannel.write(writeBuffer);

      if (!writeBuffer.hasRemaining()) {
        writeBuffers.remove(url);
        key.interestOps(SelectionKey.OP_READ);
      }

      updateState(url);

      // LOG.info(url + ": requested");
    }

    /**
     * 从服务器读取响应
     * 
     * @param key
     *          选择键
     * @throws IOException
     */
    private void read(SelectionKey key) throws IOException {
      FetchItem att = (FetchItem) key.attachment();
      String url = att.getUrl().toString();
      SocketChannel socketChannel = (SocketChannel) key.channel();

      readBuffer.clear();
      int numRead = 0;

      numRead = socketChannel.read(readBuffer);

      updateState(url);

      if (numRead > 0) {
        streams.get(url).write(readBuffer.array(), 0, numRead);
      } else if (numRead == -1) {
        ByteArrayOutputStream stream = streams.remove(url);
        finishChannel(key, CrawlDatum.STATUS_FETCH_SUCCESS,
            stream.toByteArray());

        // LOG.info(url + ": finished***");
      }
    }

    /**
     * 完成和关闭通道
     * 
     * @param key
     *          选择键
     * @param status
     *          结果状态
     * @param bytes
     *          服务器响应数据
     */
    private void finishChannel(SelectionKey key, int status, byte[] bytes) {
      FetchItem att = (FetchItem) key.attachment();
      String url = att.getUrl().toString();

      this.fetchQueues.finishFetchItem(att);

      Page page = null;
      if (bytes == null)
        page = new Page(url, att.datum);
      else
        page = new Page(url, att.datum, bytes);

      pagesQueue.add(page);
      
      if (status == CrawlDatum.STATUS_FETCH_SUCCESS)
        this.updateStatus(bytes.length);
      
      //
      // LOG.info(url + ": 通道关闭");
      try {
        key.channel().close();

        // inProgress--;
      } catch (IOException e) {
        NIOFetcher.LOG.warn(url + ": " + e);
      }

      key.cancel();
      states.remove(url);
      streams.remove(url);

      this.actives.decrementAndGet();
      inProgress--;
    }
    
    void updateStatus(int bytesInPage) {
      pages.incrementAndGet();
      bytes.addAndGet(bytesInPage);
    }

    public void run() {
      // TODO: activeThreads没有意义了，只有一个抓取线程，活动连接也许更有意义？
      this.activeThreads.incrementAndGet(); // count threads

      fetch();

      this.activeThreads.decrementAndGet(); // count threads

      NIOFetcher.LOG.info("-finishing fetcher thread, activeRequests=" + this.actives);
    }

    private void logError(Text url, String message) {
      if (NIOFetcher.LOG.isInfoEnabled()) {
        NIOFetcher.LOG.info("fetch of " + url + " failed with: " + message);
      }
      this.errors.incrementAndGet();
    }
  }