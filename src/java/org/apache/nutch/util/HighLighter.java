package org.apache.nutch.util;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.highlight.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangmingke on 16/6/24.
 */
public class HighLighter {

  private static final String regEx_scripts = "<script[^>]*?>[\\s\\S]*?<\\/script>"; // 定义script的正则表达式
  private static final String regEx_styles = "<style[^>]*?>[\\s\\S]*?<\\/style>"; // 定义style的正则表达式

  public String HighLight(String query, String text, Analyzer analyzer,
      String styleclass) throws IOException {
    // SmartChineseAnalyzer analyzer = new SmartChineseAnalyzer();
   
    TokenStream tokenStreamKeyword = analyzer.tokenStream("field", query);
    tokenStreamKeyword.reset();
    tokenStreamKeyword.addAttribute(CharTermAttribute.class);
    List<String> list = new ArrayList<String>();

    while (tokenStreamKeyword.incrementToken()) {
      list.add(
          tokenStreamKeyword.getAttribute(CharTermAttribute.class).toString());
    }
    tokenStreamKeyword.close();

    SimpleHTMLFormatter simpleHtmlFormatter = new SimpleHTMLFormatter(
        "<span class=\"" + styleclass + "\">", "</span>");
    // int i = 0;
    int length = list.size();
    String end = null;

    for (int i = 0; i < length; i++) {
      if (text.indexOf(list.get(i)) != -1) {
        Term term = new Term("field", list.get(i));
        Query query1 = new TermQuery(term);

        Highlighter highlighter = new Highlighter(simpleHtmlFormatter,
            new QueryScorer(query1));
        highlighter.setTextFragmenter(new SimpleFragmenter(Integer.MAX_VALUE));
        try {
          TokenStream tokenStream = analyzer.tokenStream("field", text);
          end = highlighter.getBestFragment(tokenStream, text);
          text = end;
        } catch (InvalidTokenOffsetsException e) {
          e.printStackTrace();
        }
      }
    }
    // System.out.println(text);
    return text;
  }

  public String HtmlHighLight(String query, String htmlStr, Analyzer analyzer,
      String styleclass) throws IOException {
    int i = 0;
    int start = 0;
    int end = 0;
    char[] htmlStrArr = htmlStr.toCharArray();
    String html = null;
    StringBuffer content = new StringBuffer();

    while (i < htmlStr.length()) {
      start = i;
      if (htmlStrArr[i] == '<') { // 标签内
        String htmlTag = htmlStr.substring(start);

        if (htmlStr.startsWith("<script", start)) { // script 标签
          Pattern p_script = Pattern.compile(regEx_scripts,
              Pattern.CASE_INSENSITIVE);
          Matcher m_script = p_script.matcher(htmlTag);
          if (m_script.find()) {
            int endScript = m_script.end();
            end = start + endScript;
          }
        } else if (htmlStr.startsWith("<style", start)) { // style标签
          Pattern p_style = Pattern.compile(regEx_styles,
              Pattern.CASE_INSENSITIVE);
          Matcher m_style = p_style.matcher(htmlTag);
          if (m_style.find()) {
            int endStyle = m_style.end();
            end = start + endStyle;
          }
        } else { // 其他HTML标签
          end = htmlStr.indexOf(">", start) + 1;
        }
        if (end < 0 || end > htmlStr.length()) {
          break;
        }
        html = htmlStr.substring(start, end);
        i = end;

      } else { // 标签外的文本加亮
        end = htmlStr.indexOf("<", start);
        if (end < 0 || end > htmlStr.length()) {
          break;
        }
        String str = htmlStr.substring(start, end);
        try {
          if (str != null) {
            html = HighLight(query, str, analyzer, styleclass);
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
        i = end;
      }
      // System.out.println("i : " + i + " start : " + start + " end:" + end);
      // System.out.println("=========" + html + "=================");
      if (html != null) {
        content.append(html);
      }
    }

    htmlStr = content.toString();
    return htmlStr;
  }

  public static void main(String[] args) throws IOException {
    String text = " <style>博客园     </style>博客园         <a  title=\"刷新博文列表\"     >刷新</a> <script>我的博客园</script> <script tyle =\"博客园\">博客园博客园</script>博家</a>";
    String query = "博客园";
    File file = new File(
        "/home/witim/workspace/nutchtest_1017/xnutch/outcontent/dump");
    if (!file.exists() || file.isDirectory())
      throw new FileNotFoundException();
    BufferedReader br = new BufferedReader(new FileReader(file));
    String temp = null;
    StringBuffer sb = new StringBuffer();
    temp = br.readLine();
    while (temp != null) {
      sb.append(temp + " ");
      temp = br.readLine();
    }
    String content1 = sb.toString();

    System.out.println(text);
    HighLighter h1 = new HighLighter();
    SmartChineseAnalyzer analyzer = new SmartChineseAnalyzer();
    String styleclass = "";  //这里的styleclass应该是一个css 的class名
    String content = h1.HtmlHighLight(query, text, analyzer, styleclass);
    System.out.println("============================");
    System.out.println(content);

    System.out.println(content1.length());
     String content11 = h1.HtmlHighLight(query, content1, analyzer, styleclass);

  }
}
