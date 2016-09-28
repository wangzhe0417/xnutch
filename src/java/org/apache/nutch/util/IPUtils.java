package org.apache.nutch.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.regex.Pattern;

import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Nutch;

public class IPUtils {
  private static Pattern IP_PATTERN = Pattern
      .compile("(\\d{1,3}\\.){3}(\\d{1,3})");

  public static boolean isIPString(String str) {
    if (IP_PATTERN.matcher(str).matches())
      return true;
    else
      return false;
  }

  public static InetAddress toIP(String str) throws UnknownHostException {
    String[] digits = str.split("\\.");
    byte[] bytes = new byte[4];
    int i = 0;
    for (String d : digits) {
      bytes[i] = (byte) Integer.parseInt(d);
      i++;
    }

    return InetAddress.getByAddress(bytes);
  }

  public static String toDotString(InetAddress ip) {
    byte[] bytes = ip.getAddress();
    String str = new String();
    for (int i = 0; i < 4; i++) {
      int b = bytes[i];
      if (b < 0)
        b += 256;
      str += b;
      if (i < 3)
        str += ".";
    }

    return str;
  }

  public static InetAddress getIP(CrawlDatum datum) throws UnknownHostException {
    InetAddress[] ips = getIPs(datum);
    if(ips != null)
      return ips[0];
    else
      return null;
  }
  
  public static String getIPString(CrawlDatum datum) throws UnknownHostException {
    InetAddress[] ips = getIPs(datum);
    if(ips != null)
      return toDotString(ips[0]);
    else
      return null;
  }

  public static String getIPStrings(CrawlDatum datum)
      throws UnknownHostException {
    if (datum.getMetaData().containsKey(Nutch.WRITABLE_IP_KEY)) {
      Text textIPs = (Text) datum.getMetaData().get(Nutch.WRITABLE_IP_KEY);

      return textIPs.toString();
    } else
      return "";
  }

  public static InetAddress[] getIPs(CrawlDatum datum)
      throws UnknownHostException {
    if (datum.getMetaData().containsKey(Nutch.WRITABLE_IP_KEY)) {
      Text textIPs = (Text) datum.getMetaData().get(Nutch.WRITABLE_IP_KEY);

      String ips = textIPs.toString();
      String[] arrIP = ips.split(",");
      InetAddress[] ias = new InetAddress[arrIP.length];
      for (int i = 0; i < arrIP.length; i++) {
        ias[i] = toIP(arrIP[i]);
      }

      return ias;
    } else
      return null;
  }
  
  public static boolean hasIP(CrawlDatum datum){
    if (datum.getMetaData().containsKey(Nutch.WRITABLE_IP_KEY))
      return true;
    else
      return false;
  }
  
  public static byte[] calculateSignature(CrawlDatum datum) throws UnknownHostException{
    String strs = getIPStrings(datum);
    if(strs.length() == 0)
      return null;
    else{
      return MD5Hash.digest(strs.getBytes()).getDigest();
    }
  }

  public static void setIP(CrawlDatum datum, InetAddress[] ips) {
    ArrayList<String> ipStrs = new ArrayList<String>();
    for (InetAddress ip : ips) {
      String dotString = toDotString(ip);
      ipStrs.add(dotString);
    }

    Collections.sort(ipStrs);
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < ipStrs.size(); i++) {
      if (i != 0)
        buf.append("," + ipStrs.get(i));
      else
        buf.append(ipStrs.get(i));
    }

    datum.getMetaData().put(Nutch.WRITABLE_IP_KEY, new Text(buf.toString()));
  }

  public static void setIP(CrawlDatum datum, InetAddress ip) {
    String dotString = toDotString(ip);

    datum.getMetaData().put(Nutch.WRITABLE_IP_KEY, new Text(dotString));
  }
  
  public static void setIP(CrawlDatum datum, String ip) {
    datum.getMetaData().put(Nutch.WRITABLE_IP_KEY, new Text(ip));
  }

  public static void main(String[] args) throws UnknownHostException {
    InetAddress ip = InetAddress.getByName("www.pku.edu.cn");
    System.out.println(ip);

    String str = IPUtils.toDotString(ip);
    System.out.println(str);

    boolean b = IPUtils.isIPString(str);
    System.out.println(b);

    ip = IPUtils.toIP(str);
    System.out.println(ip);
  }
}
