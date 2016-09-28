
<%@ page session="false" contentType="text/html; charset=UTF-8"
	import="java.io.*" import="java.util.*" import="java.text.*"
	import="org.apache.nutch.searcher.*"
	import="org.apache.nutch.parse.ParseData"
	import="org.apache.nutch.metadata.Metadata"
	import="org.apache.nutch.metadata.Nutch"
	import="org.apache.hadoop.conf.Configuration"
	import="org.apache.nutch.util.NutchConfiguration"%>
<%@ page import="org.apache.nutch.util.HighLighter"%>
<%@ page
	import="org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer"%>

<%
  Configuration nutchConf = NutchConfiguration.get(application);
			NutchBean bean = NutchBean.get(application, nutchConf);
			bean.LOG.info("cache request from " + request.getRemoteAddr());

			request.setCharacterEncoding("UTF-8");
			String queryString = request.getParameter("queryString");
			String queryLang = request.getParameter("queryLang");
			bean.LOG.info("queryString: " + queryString);
			bean.LOG.info("queryLang: " + queryLang);
			//Query query = Query.parse(queryString, queryLang, nutchConf);
			//bean.LOG.info("query: " + query.toString());

			Hit hit = new Hit(Integer.parseInt(request.getParameter("idx")), request.getParameter("id"));
			HitDetails details = bean.getDetails(hit);
			long time = bean.getFetchDate(details);
			Date date = new Date(time);
			DateFormat df = new SimpleDateFormat("yyyy.MM.dd. HH:mm:ss");
			String url = details.getValue("url");
			bean.LOG.info("cache request " + url);
			String id = "idx=" + hit.getIndexNo() + "&id=" + hit.getUniqueKey();

			HighLighter highLighter = new HighLighter();
			SmartChineseAnalyzer analyzer = new SmartChineseAnalyzer();

			String language = ResourceBundle.getBundle("org.nutch.jsp.cached", request.getLocale()).getLocale()
					.getLanguage();
			String requestURI = HttpUtils.getRequestURL(request).toString();
			String base = requestURI.substring(0, requestURI.lastIndexOf('/'));
			ParseData parseData = bean.getParseData(details);
			//Metadata metaData = bean.getParseData(details).getParseMeta();

			String content = null;
			String end = null;
			//String contentType = (String) metaData.get(Metadata.CONTENT_TYPE);
			String contentType = parseData.getMeta(Metadata.CONTENT_TYPE);
			if (contentType.startsWith("text/html")) {
				// FIXME : it's better to emit the original 'byte' sequence 
				// with 'charset' set to the value of 'CharEncoding',
				// but I don't know how to emit 'byte sequence' in JSP.
				// out.getOutputStream().write(bean.getContent(details)) may work, 
				// but I'm not sure.
				//String encoding = (String) metaData.get("CharEncodingForConversion"); 
				String encoding = parseData.getMeta("CharEncodingForConversion");
				//String encoding = parseData.getMeta(Metadata.CONTENT_ENCODING); 
				if (encoding != null) {
					try {
						content = new String(bean.getContent(details), encoding);
					} catch (UnsupportedEncodingException e) {
						// fallback to windows-1252
						content = new String(bean.getContent(details), "windows-1252");
					}
				} else
					content = new String(bean.getContent(details));
			}
%>
<!--
<base href="<%=url%>">
-->
<%@ taglib uri="http://jakarta.apache.org/taglibs/i18n" prefix="i18n"%>
<i18n:bundle baseName="org.nutch.jsp.cached" />

<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<base href="<%=base + "/" + language%>/">
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
	color: black; 
	background-color: #ffff66; 
	padding: 0.3px; 
	font-weight: bold;
}

.highlighter{
	color: #FF0000; 
	background-color: #ffff66; 
	padding: 0.3px; 
	font-weight: bold;
}
</style>
<title>Kidden: <i18n:message key="title" /></title>
<jsp:include page="style/style.html" />
<%
  out.flush();
%>

<div id="bd_snap">
	<div id="bd_snap_head">
		<a href="http://localhost:8080/nutch" id="bd_snap_logo"
			title="Kidden首页"></a>
	</div>
	<div id="bd_snap_txt">
		<i18n:message key="keywordsTips" />
		<span><a
			style='color: black; background-color: #ffff66; padding: 0 3px; font-weight: bold'><%=queryString%></a></span>
		。 <span style="margin-left: 5px"> <i18n:message
				key="FetchTimeTips">
				<i18n:messageArg value="<%=df.format(date)%>" />
			</i18n:message>
		</span> <a href="<%=url%>" dir="ltr"><i18n:message key="ThePage" /></a>
		<i18n:message key="UpdateTips" />
	</div>
	<script>
		document.getElementById("bd_tousu").href = "http://tousu.baidu.com/webmaster/add?link="
				+ encodeURIComponent(document.location);
	</script>
	<div id="bd_snap_note">
		<i18n:message key="Disclaimer1" />
		<a href="<%=url%>"><%=url%></a>
		<i18n:message key="Disclaimer2" />
	</div>
</div>
<div id="bd_snap_search">
	<form name="search" action="../search.jsp" method="get">
		<input name="query" id="bd_snap_kw" maxlength="100"  value="<%=queryString%>"><span
			id="bd_snap_btn_wr">
			<input type="hidden" name="lang" value="<%=queryLang%>">
			<input type="submit" id="bd_snap_su" value="Kidden搜索" >
			</span>
	</form>
</div>
<div id="bd_snap_ln"></div>

<div style="position: relative">
	<%
	  String caching = details.getValue("cache");
	  if (caching != null && !caching.equals(Nutch.CACHING_FORBIDDEN_NONE)) {
	%>
	<i18n:message key="noContent" />
	<%
	  return;
	  }
	%>
	<%
	  if (contentType.startsWith("text/")) {
	%>

	<%
	  if (content != null && !content.equals("")) {
	      end = highLighter.HtmlHighLight(queryString, content, analyzer,"highlighterquery");
	%>
	<%
	  if (end != null) {
	%>
	<%=end%>
	<%
	  }
	    } else {
	%>
	<i18n:message key="noContent" />
	<%
	  }
	%>

	<%
	  } else {
	%>
	<i18n:message key="CacheTips1">
		<i18n:messageArg value="<%=contentType%>" />
	</i18n:message>
	<a href="./servlet/cached?<%=id%>"> <i18n:message key="Link" />
	</a>
	<i18n:message key="CacheTips2" />

	<%
	  }
	%>
</div>