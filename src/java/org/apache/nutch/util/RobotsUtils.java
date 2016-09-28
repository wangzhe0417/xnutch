package org.apache.nutch.util;

import crawlercommons.robots.*;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Nutch;
import java.net.UnknownHostException;

/**
 * @author wangzhe
 * @create 2016-07-08-14:04
 */

public class RobotsUtils {

  /**
   * BaseRobotRules类型转换工具,String类型转换为BaseRobotRules类型
   *
   * @param url
   * @param str
   * @param agent
   * @return
   */
  public static BaseRobotRules toRules(String url, String str, String agent) {
    byte[] content = str.getBytes();
    SimpleRobotRulesParser parser = new SimpleRobotRulesParser();
    BaseRobotRules rules = parser.parseContent(url, content, 
    		"text/plain", agent);
    return rules;
  }

  /**
   * 存储String类型的Robots.txt
   *
   * @param datum
   * @param rules
   */
  public static void setRobots(CrawlDatum datum, String rules) {
    datum.getMetaData().put(Nutch.WRITABLE_ROBOTS_KEY, new Text(rules));
  }

  /**
   * 判断是否存在Robots.txt
   *
   * @param datum
   * @return
   */
  public static boolean hasRobots(CrawlDatum datum) {
    if (datum.getMetaData().containsKey(Nutch.WRITABLE_ROBOTS_KEY))
      return true;
    else
      return false;
  }

  /**
   * 获取robots.txt
   *
   * @param datum
   * @return
   * @throws UnknownHostException
   */
  public static BaseRobotRules getRobots(String url, CrawlDatum datum,
      String agent) throws UnknownHostException {
    String strRule = getRobotsString(datum);
    if (strRule != null) {
      BaseRobotRules rules = toRules(url, strRule, agent);
      return rules;
    } else
      return null;

  }

  /**
   * 获取String类型的robots.txt
   *
   * @param datum
   * @return
   */
  public static String getRobotsString(CrawlDatum datum) {
    if (datum.getMetaData().containsKey(Nutch.WRITABLE_ROBOTS_KEY)) {
      Text textRules = (Text) datum.getMetaData()
          .get(Nutch.WRITABLE_ROBOTS_KEY);
      String strRule = textRules.toString();
      return strRule;
    } else
      return null;
  }
}
  // public static byte[] calculateSignature(CrawlDatum datum)
  // throws UnknownHostException {
  // String strs = getRobotsString(datum);
  // if (strs.length() == 0)
  // return null;
  // else {
  // return MD5Hash.digest(strs.getBytes()).getDigest();
  // }
  // }