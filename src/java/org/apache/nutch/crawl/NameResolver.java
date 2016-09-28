package org.apache.nutch.crawl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;

import org.xbill.DNS.ARecord;
import org.xbill.DNS.DClass;
import org.xbill.DNS.Message;
import org.xbill.DNS.Name;
import org.xbill.DNS.RRset;
import org.xbill.DNS.Record;
import org.xbill.DNS.Section;
import org.xbill.DNS.SimpleResolver;
import org.xbill.DNS.Type;

/**
 * 使用dnsjava进行域名解析
 * 
 * @author kidden
 *
 */
public class NameResolver {
  private SimpleResolver resolver;
  private String server = "";

  /**
   * 使用系统的缺省域名服务器进行域名解析
   * 
   * @param timeout
   *          解析超时
   * @throws UnknownHostException
   *           若缺省域名服务器不可知
   */
  public NameResolver(int timeout) throws UnknownHostException {
    resolver = new SimpleResolver();
    resolver.setTimeout(timeout);
  }

  /**
   * 使用给定域名服务器进行域名解析
   * 
   * @param dnsServer
   *          域名解析的服务器
   * @param timeout
   *          解析超时
   * @throws UnknownHostException
   *           若服务器不可知
   */
  public NameResolver(String dnsServer, int timeout)
      throws UnknownHostException {
    this.server = dnsServer;

    resolver = new SimpleResolver(dnsServer);
    resolver.setTimeout(timeout);
  }

  /**
   * 域名解析所使用的服务器
   * 
   * @return 当使用缺省服务器时，返回空
   */
  public String dnsServer() {
    return server;
  }

  /**
   * 获得主机名对应的ip地址列表
   * 
   * @param host
   *          待解析主机
   * @return 主机名对应的ip地址列表
   * @throws IOException
   *           若解析失败
   */
  public InetAddress[] resolve(String host) throws IOException {
    Name name = Name.fromString(host, Name.root);
    Record rec = Record.newRecord(name, Type.A, DClass.IN);
    Message query = Message.newQuery(rec);

    Message response = resolver.send(query);

    RRset[] rset = response.getSectionRRsets(Section.ANSWER);
    ArrayList<InetAddress> ips = new ArrayList<InetAddress>();
    for (RRset r : rset) {
      Iterator t = r.rrs(false);
      while (t.hasNext()) {
        Record d = (Record) t.next();
        if (d instanceof ARecord) {
          ARecord ad = (ARecord) d;

          ips.add(ad.getAddress());
        }
      }
    }

    return ips.toArray(new InetAddress[0]);
  }

  public static void main(String[] args) throws IOException {
    String dns = args[0];
    String host = args[1];

    NameResolver resolver = new NameResolver(dns, 30);

    InetAddress[] ips = resolver.resolve(host);
    System.out.println(ips.length);
    for (InetAddress ip : ips)
      System.out.println(ip);
  }
}