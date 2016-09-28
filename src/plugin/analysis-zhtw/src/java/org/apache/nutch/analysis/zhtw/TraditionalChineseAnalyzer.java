package org.apache.nutch.analysis.zhtw;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.lionsoul.jcseg.analyzer.JcsegAnalyzer4X;
import org.lionsoul.jcseg.core.JcsegTaskConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import org.apache.nutch.analysis.NutchAnalyzer;

/**
 * Created by zhangmingke on 16/8/8.
 */
public class TraditionalChineseAnalyzer extends NutchAnalyzer {

  private static JcsegTaskConfig config;
  private static JcsegAnalyzer4X jcsegAnalyzer;
  public static Logger LOG = LoggerFactory
      .getLogger(TraditionalChineseAnalyzer.class);

  public TraditionalChineseAnalyzer() throws IOException {

    jcsegAnalyzer = new JcsegAnalyzer4X(JcsegTaskConfig.COMPLEX_MODE);
    LOG.info("繁体分词器");
    config = jcsegAnalyzer.getTaskConfig();
    config.resetFromPropertyFile("conf/jcseg.properties");
    //在conf里面的jcseg.properties里的lexicon.path这一项里面输入lexicon包的绝对路径
    //就像范例中jcseg.properties使用的那样
    config.setAutoload(true);
  }

  @Override
  protected TokenStreamComponents createComponents(String fieldName,
      Reader reader) {

    try {

     // jcsegAnalyzer.setConfig(config);
      jcsegAnalyzer.tokenStream(fieldName, reader);
      LOG.info("繁体分词器");
      TokenStreamComponents tokenStreamComponents = jcsegAnalyzer
          .getReuseStrategy().getReusableComponents(jcsegAnalyzer, fieldName);
      return tokenStreamComponents;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;

  }

  public static void main(String[] args) throws IOException {
    TraditionalChineseAnalyzer traditionalChineseAnalyzer = new TraditionalChineseAnalyzer();
    TokenStream tokenStream = traditionalChineseAnalyzer.tokenStream("a",
        "優雅的烏龜,裏約奧運會，澳大利亞素質好差");
    tokenStream.reset();
    while (tokenStream.incrementToken()) {
      System.out.println(
          tokenStream.getAttribute(CharTermAttribute.class).toString());
    }
    tokenStream.close();

  }
}
