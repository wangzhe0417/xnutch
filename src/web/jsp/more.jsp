<%--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
  
  http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
--%>
<%
    // @author John Xing
    // show meta info (currently type, size, date of last-modified)
    // for each hit. These info are indexed by ./src/plugin/index-more.

    // do not show unless we have something
    boolean showMore = true;
 
    String contentType = subType;
    if (contentType == null)
      contentType = primaryType;
    if (contentType != null && !("text".equalsIgnoreCase(primaryType) || type.equalsIgnoreCase("application/xhtml+xml"))) {
      contentType = "<span class=\"contentType\">" + contentType + "</span>";
      showMore = true;
    } else {
      contentType = "";
    }

    // Content-Length
   // String contentLength = detail.getValue("contentLength");
   // if (contentLength != null) {
   // 	long len = Long.valueOf(contentLength);
   //   contentLength = "(" + org.apache.hadoop.util.StringUtils.byteDesc(len) + ")";
   //   showMore = true;
   // } else {
   //   contentLength = "";
   // }

    // Last-Modified
   // String lastModified = detail.getValue("lastModified");
   // if (lastModified != null) {
   //   Calendar cal = new GregorianCalendar();
   //   cal.setTimeInMillis(new Long(lastModified).longValue());
    //  lastModified = cal.get(Calendar.YEAR)
   //               + "." + (1+cal.get(Calendar.MONTH)) // it is 0-based
   //               + "." + cal.get(Calendar.DAY_OF_MONTH);
  //    showMore = true;
  //  } else {
  //    lastModified = "";
    //}
%>

<% if (showMore) {%>
    <br><font size=-1><nobr><%=contentType%> - <a href="../text.jsp?<%=id%>&queryString=<%=queryString%>&queryLang=<%=queryLang%>" target="_blank"><i18n:message key="viewAsText"/></a></nobr></font>
<%  } %>