package org.apache.nutch.fetcher;

import org.apache.hadoop.io.Writable;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.protocol.ProtocolStatus;
import org.apache.nutch.util.DeflateUtils;
import org.apache.nutch.util.GZIPUtils;

import java.io.*;


public class Page {
  private String uri;
  private CrawlDatum datum;

  private byte[] data;

  private byte[] content;
  private int code;
  private Metadata headers = new Metadata();

  private final static int BUFFER_SIZE = 8192;

  public static Page EndPage = new Page("end://end", null);

  public Page(String uri, CrawlDatum datum, byte[] data) {
    this.uri = uri;
    this.data = data;
    this.datum = datum;
  }

  public Page(String uri, CrawlDatum datum) {
    this.uri = uri;
    this.data = null;
    this.datum = datum;
  }

  public boolean isEndPage() {
    return uri.equals("end://end");
  }

  public boolean isFetchFailed() {
    return data == null;
  }

  public boolean isRobotsDenied() {
    Writable status = datum.getMetaData().get(Nutch.WRITABLE_PROTO_STATUS_KEY);
    if (status == null)
      return false;
    else {
      return status.equals(ProtocolStatus.STATUS_ROBOTS_DENIED);
    }
  }

  public CrawlDatum getDatum() {
    return datum;
  }

  public void process() throws IOException {
    if (isFetchFailed())
      return;

    PushbackInputStream in = // process response
    new PushbackInputStream(new BufferedInputStream(new ByteArrayInputStream(
        data), BUFFER_SIZE), BUFFER_SIZE);

    StringBuffer line = new StringBuffer();

    boolean haveSeenNonContinueStatus = false;
    while (!haveSeenNonContinueStatus) {
      // parse status code line
      this.code = parseStatusLine(in, line);
      // parse headers
      parseHeaders(in, line);
      haveSeenNonContinueStatus = code != 100; // 100 is "Continue"
    }

    readPlainContent(in);

    String contentEncoding = getHeader("Content-Encoding");
    if ("gzip".equals(contentEncoding) || "x-gzip".equals(contentEncoding)) {
      content = processGzipEncoded(content);
    } else if ("deflate".equals(contentEncoding)) {
      content = processDeflateEncoded(content);
    } else {
    }
  }

  public byte[] processGzipEncoded(byte[] compressed) throws IOException {
    byte[] content;

    // TODO: 非GZIP格式如何产生？如何处理？
    content = GZIPUtils.unzipBestEffort(compressed);

    if (content == null)
      throw new IOException("unzipBestEffort returned null");

    return content;
  }

  public byte[] processDeflateEncoded(byte[] compressed) throws IOException {
    byte[] content = DeflateUtils.inflateBestEffort(compressed);

    if (content == null)
      throw new IOException("inflateBestEffort returned null");

    return content;
  }

  private void readPlainContent(InputStream in) throws IOException {

    int contentLength = Integer.MAX_VALUE; // get content length
    String contentLengthString = headers.get("Content-Length");
    if (contentLengthString != null) {
      contentLengthString = contentLengthString.trim();
      try {
        contentLength = Integer.parseInt(contentLengthString);
      } catch (NumberFormatException e) {
        throw new IOException("bad content length: " + contentLengthString);
      }
    }
    // if (http.getMaxContent() >= 0
    // && contentLength > http.getMaxContent()) // limit download size
    // contentLength = http.getMaxContent();

    ByteArrayOutputStream out = new ByteArrayOutputStream(BUFFER_SIZE);
    byte[] bytes = new byte[BUFFER_SIZE];
    int length = 0; // read content
    for (int i = in.read(bytes); i != -1; i = in.read(bytes)) {

      out.write(bytes, 0, i);
      length += i;
      if (length >= contentLength)
        break;
    }
    content = out.toByteArray();
  }

  private int parseStatusLine(PushbackInputStream in, StringBuffer line)
      throws IOException {
    readLine(in, line, false);

    int codeStart = line.indexOf(" ");
    int codeEnd = line.indexOf(" ", codeStart + 1);

    // handle lines with no plaintext result code, ie:
    // "HTTP/1.1 200" vs "HTTP/1.1 200 OK"
    if (codeEnd == -1)
      codeEnd = line.length();

    int code;
    try {
      code = Integer.parseInt(line.substring(codeStart + 1, codeEnd));
    } catch (NumberFormatException e) {
      throw new IOException(
          "bad status line '" + line + "': " + e.getMessage(), e);
    }

    return code;
  }

  private void processHeaderLine(StringBuffer line) throws IOException {

    int colonIndex = line.indexOf(":"); // key is up to colon
    if (colonIndex == -1) {
      int i;
      for (i = 0; i < line.length(); i++)
        if (!Character.isWhitespace(line.charAt(i)))
          break;
      if (i == line.length())
        return;
      throw new IOException("No colon in header:" + line);
    }
    String key = line.substring(0, colonIndex);

    int valueStart = colonIndex + 1; // skip whitespace
    while (valueStart < line.length()) {
      int c = line.charAt(valueStart);
      if (c != ' ' && c != '\t')
        break;
      valueStart++;
    }
    String value = line.substring(valueStart);
    headers.set(key, value);
  }

  // Adds headers to our headers Metadata
  private void parseHeaders(PushbackInputStream in, StringBuffer line)
      throws IOException {

    while (readLine(in, line, true) != 0) {

      // handle HTTP responses with missing blank line after headers
      int pos;
      if (((pos = line.indexOf("<!DOCTYPE")) != -1)
          || ((pos = line.indexOf("<HTML")) != -1)
          || ((pos = line.indexOf("<html")) != -1)) {

        in.unread(line.substring(pos).getBytes("UTF-8"));
        line.setLength(pos);

        try {
          // TODO: (CM) We don't know the header names here
          // since we're just handling them generically. It would
          // be nice to provide some sort of mapping function here
          // for the returned header names to the standard metadata
          // names in the ParseData class
          processHeaderLine(line);
        } catch (Exception e) {

        }
        return;
      }

      processHeaderLine(line);
    }
  }

  private static int readLine(PushbackInputStream in, StringBuffer line,
      boolean allowContinuedLine) throws IOException {
    line.setLength(0);
    for (int c = in.read(); c != -1; c = in.read()) {
      switch (c) {
      case '\r':
        if (peek(in) == '\n') {
          in.read();
        }
      case '\n':
        if (line.length() > 0) {
          // at EOL -- check for continued line if the current
          // (possibly continued) line wasn't blank
          if (allowContinuedLine)
            switch (peek(in)) {
            case ' ':
            case '\t': // line is continued
              in.read();
              continue;
            }
        }
        return line.length(); // else complete
      default:
        line.append((char) c);
      }
    }
    throw new EOFException();
  }

  private static int peek(PushbackInputStream in) throws IOException {
    int value = in.read();
    in.unread(value);
    return value;
  }

  public String getUri() {
    return uri;
  }

  public int getStatusCode() {
    return code;
  }

  public Metadata getHeaders() {
    return headers;
  }

  public byte[] getContent() {
    return content;
  }

  public String getHeader(String name) {
    return headers.get(name);
  }
}
