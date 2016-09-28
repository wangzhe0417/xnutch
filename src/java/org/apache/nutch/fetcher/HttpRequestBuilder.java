package org.apache.nutch.fetcher;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.lang.StringUtils;

public class HttpRequestBuilder {
  private static String GET_REQUEST_HEADER = "GET %s HTTP/1.0\r\n";

  private static String FIELD_ENCODING = "%s: %s\r\n";

  public String buildGet(String url) throws MalformedURLException {
    URL u = new URL(url);
    return encodeHeader(u) + encodeField("Host", u.getHost()) +
    // encodeField("Connection", "close") +
    // encodeField("Accept-Language", "en-us,en-gb,en;q=0.7,*;q=0.3") +
     encodeField("Accept-Encoding", "x-gzip, gzip, deflate") +
        "\r\n";
  }

  private String encodeHeader(URL url) {
    String path = url.getPath();
    if (StringUtils.isBlank(path)) {
      path = "/";
    }
    return String.format(GET_REQUEST_HEADER, path);
  }

  private String encodeField(String field, String value) {
    return String.format(FIELD_ENCODING, field, value);
  }
}
