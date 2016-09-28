
<%@ page session="false" contentType="text/html; charset=UTF-8"
	import="java.io.*" import="java.util.*"
	import="org.apache.nutch.searcher.*"
	import="org.apache.hadoop.conf.Configuration"
	import="org.apache.nutch.util.NutchConfiguration"%>
<%@ page import="org.apache.nutch.util.HighLighter"%>
<%@ page
	import="org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer"%>

<%
  // show the content of a hit as plain text
			Configuration nutchConf = NutchConfiguration.get(application);
			NutchBean bean = NutchBean.get(application, nutchConf);
			request.setCharacterEncoding("UTF-8");

			bean.LOG.info("text request from " + request.getRemoteAddr());

			String queryString = request.getParameter("queryString");
			String queryLang = request.getParameter("queryLang");

			//Query query = new Query(queryString);

			Hit hit = new Hit(Integer.parseInt(request.getParameter("idx")), request.getParameter("id"));
			HitDetails details = bean.getDetails(hit);

			String text = bean.getParseText(details).getText();

			HighLighter highLighter = new HighLighter();

			String language = ResourceBundle.getBundle("org.nutch.jsp.text", request.getLocale()).getLocale()
					.getLanguage();
			String requestURI = HttpUtils.getRequestURL(request).toString();
			String base = requestURI.substring(0, requestURI.lastIndexOf('/'));
			SmartChineseAnalyzer analyzer = new SmartChineseAnalyzer();
			// 20041005, xing
			// This "CharEncodingForConversion" thing is only pertinent to
			// html parser (plugin parse-html) in current nutch. None of
			// other parser plugins are into it. So we worry it later.
%>
<jsp:include page="style/style.html" />
<base href="<%=base + "/" + language%>/">
<%
  out.flush();
%>

<%@ taglib uri="http://jakarta.apache.org/taglibs/i18n" prefix="i18n"%>
<i18n:bundle baseName="org.nutch.jsp.text" />
<title>Kidden: <i18n:message key="title" /></title>
<style>
body {
	position: relative
}

body, form {
	margin: 0 !important;
	padding: 0 !important
}

#bd_snap {
	font: 14px arial;
	color: #000;
	background: #fff;
	text-align: left;
	padding: 7px 0 0 20px
}

#bd_snap_txt {
	clear: both;
	padding: 10px 0
}

#bd_snap_note {
	font-size: 12px;
	color: #666;
	padding-bottom: 10px
}

#bd_snap a {
	font: 14px arial;
	color: #00c;
	text-decoration: underline
}

#bd_snap_head {
	width: 860px;
	height: 44px
}

#bd_snap_logo {
	width: 162px;
	height: 38px;
	display: block;
	background: url(http://localhost:8080/nutch/img/logo_small.gif)
		no-repeat;
	margin-right: 15px;
	float: left
}

#bd_snap_search {
	width: 680px;
	position: absolute;
	left: 170px;
	top: 17px
}

#bd_snap_kw {
	width: 519px;
	height: 22px;
	padding: 4px 7px;
	padding: 6px 7px 2px\9;
	margin-right: 5px;
	font: 16px arial;
	background: url(http://www.baidu.com/img/i-1.0.0.png) no-repeat -304px 0;
	_background-attachment: fixed;
	border: 1px solid #cdcdcd;
	border-color: #9a9a9a #cdcdcd #cdcdcd #9a9a9a;
	vertical-align: top
}

#bd_snap_su {
	width: 95px;
	height: 32px;
	font-size: 14px;
	color: #000;
	padding: 0;
	padding-top: 2px\9;
	border: 0;
}

input.bd_snap_btn {
	background: #ddd url(http://www.baidu.com/img/i-1.0.0.png);
	cursor: pointer
}

input.bd_snap_btn_h {
	background-position: -100px 0
}

#bd_snap_btn_wr {
	width: 97px;
	height: 34px;
	display: inline-block;
	background: url(http://www.baidu.com/img/i-1.0.0.png) no-repeat -202px 0;
	_top: 1px;
	*position: relative
}

#bd_snap_ln {
	height: 1px;
	border-top: 1px solid #ACA899;
	background: #ECE9D8;
	overflow: hidden
}

#bd_snap_txt span a {
	text-decoration: none
}

.highlighterquery{
	color: #FF0000; 
	font-weight: bold;
}
</style>

<div id="bd_snap">
	<div id="bd_snap_head">
		<a href="http://localhost:8080/nutch" id="bd_snap_logo"
			title="Kidden首页"></a>
	</div>
	<div id="bd_snap_txt">
		<i18n:message key="note">
			<i18n:messageArg value="<%=details.getValue(\"url\")%>" />
		</i18n:message>
	</div>
	<script>
		document.getElementById("bd_tousu").href = "http://tousu.baidu.com/webmaster/add?link="
				+ encodeURIComponent(document.location);
	</script>
</div>

<div id="bd_snap_search">
	<form name="search" action="../search.jsp" method="get">
		<input name="query" value="<%=queryString%>" id="bd_snap_kw"
			maxlength="100"> <span id="bd_snap_btn_wr"> <input
			type="hidden" name="lang" value="<%=queryLang%>"> <input
			type="submit" id="bd_snap_su" value="<i18n:message key="search"/>"
			class="bd_snap_btn"
			onmousedown="this.className='bd_snap_btn bd_snap_btn_h'"
			onmouseout="this.className='bd_snap_btn'">
		</span>
	</form>
</div>
<div id="bd_snap_ln"></div>

<hr>
<%
  try {
    String end = highLighter.HighLight(queryString, text, analyzer,"highlighterquery");
%>
<%
  if (end != null) {
%>
<%=end%>
<%
  } else {
%>
<i18n:message key="noText" />
<%
  }
%>
<%
  } catch (IOException e) {
    e.printStackTrace();
  }
%>

