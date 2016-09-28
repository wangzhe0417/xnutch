package org.apache.nutch.analysis;



import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;


/**
 * Created by zhangmingke on 16/8/8.
 */
public class DefaultAnalyzer extends NutchAnalyzer{

    private static StandardAnalyzer analyzer;
    public static Logger LOG = LoggerFactory.getLogger(DefaultAnalyzer.class);

    public DefaultAnalyzer(){
        analyzer = new StandardAnalyzer();
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName, Reader reader) {

        try {
            analyzer.tokenStream(fieldName, reader);
            LOG.info("标准分词器");
            TokenStreamComponents tokenStreamComponents = analyzer.getReuseStrategy().getReusableComponents(analyzer, fieldName);

            return tokenStreamComponents;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;

    }
    public static void main(String[] args) throws IOException {
        DefaultAnalyzer a = new DefaultAnalyzer();
        //TokenStream tokenStream = a.tokenStream("title", "you are a good man ok");
        TokenStream tokenStream = a.tokenStream("title", "今天是个好日子");
        tokenStream.reset();
        while (tokenStream.incrementToken()){
            System.out.println(tokenStream
                    .getAttribute(CharTermAttribute.class).toString());
        }
        tokenStream.close();
    }
}
