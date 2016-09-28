package org.apache.nutch.fetcher;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ProtocolStatus;

// / TODO: 处理HTTPS协议
class PageHandler implements Runnable {
  private BlockingQueue<Page> pagesQueue;

  private final byte[] EMPTY_CONTENT = new byte[0];
  
 private static final String PROTOCOL_REDIR = "protocol";
 
  private Configuration conf;

  private FetchOutputer fetchOutputer;

  // Used by the REST service
  private FetchNode fetchNode;
  private boolean reportToNutchServer;

  public PageHandler(BlockingQueue<Page> pagesQueue,
      FetchOutputer fetchOutputer, Configuration conf) {
    // this.nioFetcher = nioFetcher;
    this.pagesQueue = pagesQueue;
    this.conf = conf;
    this.fetchOutputer = fetchOutputer;
  }

  @Override
  public void run() {
    while (true) {
      try {
        Page page = pagesQueue.take();

        if (page.isEndPage())
          break;

        String url = page.getUri();

        // LOG.info("Handling " + url);
        page.process();

        CrawlDatum datum = page.getDatum();
        datum.setFetchTime(System.currentTimeMillis());
        if (page.isFetchFailed()) {
          fetchOutputer.output(new Text(url), datum, null,
              ProtocolStatus.STATUS_RETRY, CrawlDatum.STATUS_FETCH_RETRY);
          continue;
        }

        if (page.isRobotsDenied()) {
          fetchOutputer.output(new Text(url), datum, null, 
              ProtocolStatus.STATUS_ROBOTS_DENIED,
              CrawlDatum.STATUS_FETCH_GONE);
          continue;
        }

        int code = page.getStatusCode();
        int status = CrawlDatum.STATUS_FETCH_RETRY;
        if (code == 200) {
          status = CrawlDatum.STATUS_FETCH_SUCCESS;
        } else if (code == 410) { // page is gone
          status = CrawlDatum.STATUS_FETCH_GONE;
        } else if (code >= 300 && code < 400) { // handle redirect
          // TODO: 重定向处理
          String newUrl = page.getHeader("Location");
          if (newUrl != null) {
            try {
              fetchOutputer.handleRedirect(new Text(url), datum, url, newUrl, true,PROTOCOL_REDIR);
            } catch (IOException e) {

            }
          }
          switch (code) {
          case 300: // multiple choices, preferred value in Location
            status = CrawlDatum.STATUS_FETCH_REDIR_PERM;
            break;
          case 301: // moved permanently
            status = CrawlDatum.STATUS_FETCH_REDIR_PERM;
            break;
          case 305: // use proxy (Location is URL of proxy)
            status = CrawlDatum.STATUS_FETCH_REDIR_PERM;
            break;
          case 302: // found (temporarily moved)
            status = CrawlDatum.STATUS_FETCH_REDIR_TEMP;
            break;
          case 303: // see other (redirect after POST)
            status = CrawlDatum.STATUS_FETCH_REDIR_TEMP;
            break;
          case 307: // temporary redirect
            status = CrawlDatum.STATUS_DB_REDIR_TEMP;
            break;
          case 304: // not modified
            status = CrawlDatum.STATUS_FETCH_NOTMODIFIED;
            break;
          default:
            status = CrawlDatum.STATUS_FETCH_REDIR_PERM;
          }
        } else if (code == 400) { // bad request, mark as GONE
          status = CrawlDatum.STATUS_FETCH_GONE;
        } else if (code == 401) { // requires authorization, but no valid auth
                                  // provided.
          status = CrawlDatum.STATUS_FETCH_RETRY;
        } else if (code == 404) {
          status = CrawlDatum.STATUS_FETCH_GONE;
        } else if (code == 410) { // permanently GONE
          status = CrawlDatum.STATUS_FETCH_GONE;
        } else {
          status = CrawlDatum.STATUS_FETCH_RETRY;
        }

        datum.setStatus(status);

        byte[] content = page.getContent();
        Content c = new Content(page.getUri(), page.getUri(),
            (content == null ? EMPTY_CONTENT : content),
            page.getHeader("Content-Type"), page.getHeaders(), conf);

        fetchOutputer.output(new Text(url), datum, c, null, status);

      } catch (Exception e) {
        // XXX: 此处失败没有将数据保存到crawl-fetch
        NIOFetcher.LOG.warn(StringUtils.stringifyException(e));
        continue;
      }
    }

    NIOFetcher.LOG.info("退出页面处理线程");
  }

}