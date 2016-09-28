package org.apache.nutch.analysis.zhcn;

import java.io.IOException;
import java.io.Reader;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.nutch.analysis.NutchAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleChineseAnalyzer extends NutchAnalyzer {

  private static SmartChineseAnalyzer ANALYZER;

  public static Logger LOG = LoggerFactory
      .getLogger(SimpleChineseAnalyzer.class);

  public SimpleChineseAnalyzer() {
    ANALYZER = new SmartChineseAnalyzer();
  }

  @Override
  protected TokenStreamComponents createComponents(String fieldName,
      Reader reader) {
    LOG.info("简体分词器");
    return ANALYZER.createComponents(fieldName, reader);
  }

  public static void main(String[] args) throws IOException {
    SimpleChineseAnalyzer s = new SimpleChineseAnalyzer();
    TokenStream tokenStream = s.tokenStream("filed", "你知道我是好人吗");
    tokenStream.reset();
    while (tokenStream.incrementToken()) {
      System.out.println(
          tokenStream.getAttribute(CharTermAttribute.class).toString());
    }
    tokenStream.close();
  }

}