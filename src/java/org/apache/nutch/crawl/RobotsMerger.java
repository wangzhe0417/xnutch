package org.apache.nutch.crawl;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.RobotsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.Iterator;

/**
 * This class merge the robots.txt from fetch and content into tempdir
 *
 * @author wangzhe
 * @create 2016-07-19-10:47
 */

public class RobotsMerger extends MapReduceBase
    implements Mapper<Text, Writable, Text, CrawlDatum>,
    Reducer<Text, CrawlDatum, Text, CrawlDatum> {

  private CrawlDatum result = new CrawlDatum();

  public static final Logger LOG = LoggerFactory.getLogger(RobotsMerger.class);

  public void configure(JobConf job) {
  }

  public void close() {
  }

  public void map(Text url, Writable value,
      OutputCollector<Text, CrawlDatum> output, Reporter reporter)
      throws IOException {
    String urlString = url.toString();
    URL u = new URL(urlString);
    Text host = new Text(u.getHost());

    Writable val = (Writable) value;
    if (val instanceof Content) {
      Content content = ((Content) val);
      CrawlDatum datum = new CrawlDatum();
      // datum.setStatus(CrawlDatum.STATUS_DB_FETCHED);
      boolean isPlainText = (content.getContentType() != null)
          && (content.getContentType().startsWith("text/plain"));
      String rules = new String(content.getContent());
      boolean isRobots = rules.contains("User-agent");

      if (isPlainText && isRobots) {
        RobotsUtils.setRobots(datum, rules);
      }

      output.collect(host, datum);
    } else if (val instanceof CrawlDatum) {
      CrawlDatum datum = (CrawlDatum) val;

      output.collect(host, datum);
    }
  }

  public void reduce(Text host, Iterator<CrawlDatum> values,
      OutputCollector<Text, CrawlDatum> output, Reporter reporter)
      throws IOException {
    CrawlDatum fetch = new CrawlDatum();
    CrawlDatum content = new CrawlDatum();

    // 通过CrawlDatum.hasFetchStatus来判断datum来自content还是fetch
    while (values.hasNext()) {
      CrawlDatum datum = values.next();
      if (CrawlDatum.hasFetchStatus(datum)) {// 来自fetch
        fetch.set(datum);
        // fetch.getMetaData().clear();
      } else if (!CrawlDatum.hasFetchStatus(datum)) {// 来自content
        content.set(datum);
      }
    }

    result.set(fetch);
    if (RobotsUtils.hasRobots(content)) {
      String rules = RobotsUtils.getRobotsString(content);
      RobotsUtils.setRobots(result, rules);
    }

    output.collect(host, result);

  }

}
