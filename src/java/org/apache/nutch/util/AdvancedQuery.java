package org.apache.nutch.util;

import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import javax.servlet.http.HttpServletRequest;

public class AdvancedQuery {
  private static StringBuffer qStringBuffer = new StringBuffer();
  private static String queryString = "";
  private HttpServletRequest request;

  public AdvancedQuery(HttpServletRequest request) {
    this.request = request;
  }

  public String getQueryAnd() {
    String qand = request.getParameter("qand");
    String queryAnd = "";
    if (qand != "") {
      String[] str = qand.split("\\s+");
      for (int i = 0; i < str.length; i++) {
        if (str[i] == null || str[i] == "" || str[i].length() == 0) {
          continue;
        }
        queryAnd += "+" + str[i] + " ";
      }
    }
    return queryAnd;
  }

  public String getQueryPhrase() {
    String qphrase = request.getParameter("qphrase");
    if (qphrase != null && qphrase != "") {
      qphrase = "\"" + qphrase + "\" ";
    }
    return qphrase;
  }

  public String getQueryOr() {
    String qor = request.getParameter("qor");
    String queryOr = "";
    if (qor != "") {
      String[] str = qor.split("\\s+");
      for (int i = 0; i < str.length; i++) {
        if (str[i] == null || str[i] == "" || str[i].length() == 0) {
          continue;
        }
        queryOr += str[i] + " ";
      }
    }
    return queryOr;
  }

  public String getQueryNot() {
    String qnot = request.getParameter("qnot");
    String queryNot = "";
    if (qnot != "") {
      String[] str = qnot.split("\\s+");
      for (int i = 0; i < str.length; i++) {
        if (str[i] == null || str[i] == "" || str[i].length() == 0) {
          continue;
        }
        queryNot  += " -" + str[i] + " ";
      }
    }
    return queryNot;
  }

  // where: url content
  public String getWhere(String qString) {
    String where = request.getParameter("where");
    if (where != ""  && where != null && where != "any") {
      String tmp = where + ":( " + qString + " ) ";
      where = tmp;
    } else {
      where = qString;
    }
    return where;
  }

  public String getSite() {
    String site = request.getParameter("site");
    if (site != null && site != "") {
      site = " site:" + site + " ";
    }
    return null;
  }

  public String getFormat() {
    String format = request.getParameter("format");
    if (format != "" && format != null && format != "any") {
      format = " format:" + format + " ";
      return format;
    }
    return null;
  }

  public String getLanguage() {
    String lang = request.getParameter("lang");
    if (lang != "" && lang != null && lang != "any") {
      lang = " lang:" + lang + " ";
      return lang;
    }
    return null;
  }

  public String getWhen() {
    String when = request.getParameter("when");

    if (when != "" && when != null &&  when != "any") {
      int dates = Integer.valueOf(when);
      Date now = new Date();
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
      String nowTime = dateFormat.format(now);
      
      Calendar calendar = Calendar.getInstance();
      calendar.add(Calendar.DATE, -dates);
      Date startDate = calendar.getTime();
      String startTime = dateFormat.format(startDate);
      
      when = " date:[ " + startTime + " TO " + nowTime + " ] ";
      return when;
    }
    return null;
  }

  public String toQueryString() {
    qStringBuffer.setLength(0);
    if (getQueryAnd() != "")
      qStringBuffer.append(getQueryAnd());
    if (getQueryPhrase() != "")
      qStringBuffer.append(getQueryPhrase());
    if (getQueryOr() != "")
      qStringBuffer.append(getQueryOr());
    if (getQueryNot() != "")
      qStringBuffer.append(getQueryNot());

    queryString = getWhere(qStringBuffer.toString());

    if (getSite() != null) {
      queryString += getSite();
    }
    if (getFormat() != null) {
      queryString += getFormat();
    }
    if (getLanguage() != null) {
      queryString += getLanguage();
    }
    if (getWhen() != null) {
      queryString += getWhen();
    }
    return queryString;
  }

  public String getQuery() {
    return toQueryString();
  }

  public static void main(String[] args) {
    // TODO Auto-generated method stub
    String where = "";
    String qString = "aaa";
    if (where != "" && where != "any") {
      String tmp = where + " :( " + qString + " ) ";
      where = tmp;
    } else {
      where = qString;
    }
    System.out.println(where);
  }

}
