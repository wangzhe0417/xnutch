
<%@ page session="false" contentType="text/html; charset=UTF-8"
				pageEncoding="UTF-8" import="java.io.*" import="java.util.*"
				import="java.text.*" import="java.net.*"
				import="javax.servlet.http.*" import="javax.servlet.*"
				import="org.apache.nutch.util.Entities"
				import="org.apache.nutch.metadata.Nutch"
				import="org.apache.nutch.searcher.*"
				import="org.apache.nutch.plugin.*"
				import="org.apache.nutch.clustering.*"
				import="org.apache.hadoop.conf.*" 
				import="org.apache.nutch.util.NutchConfiguration"%>
<%@ page import="org.apache.nutch.util.TypeUtils" 
                    import="org.apache.nutch.util.AdvancedQuery"%>

<%!/**
	 * Maximum hits per page to be displayed.
	 */
	private int MAX_HITS_PER_PAGE;

	/**
	 * Nutch configuration for this servlet.
	 */
	private Configuration nutchConf;

	/**
	 * Initialize search bean.
	 */
	public void jspInit() {
		super.jspInit();

		final ServletContext application = getServletContext();
		nutchConf = NutchConfiguration.get(application);

		MAX_HITS_PER_PAGE = nutchConf.getInt("searcher.max.hits.per.page", -1);

	}%>


<%
	// The Nutch bean instance is initialized through a ServletContextListener 
	// that is setup in the web.xml file
	NutchBean bean = NutchBean.get(application, nutchConf);
	// set the character encoding to use when interpreting request values 
	request.setCharacterEncoding("UTF-8");

	bean.LOG.info("query request from " + request.getRemoteAddr());
	String queryString = null;
    if(request.getParameter("query") != null){
	// get query from request
	queryString = request.getParameter("query");
   }else{
//Advanced Wed Search 
	AdvancedQuery advancedQuery = new AdvancedQuery(request);
	String advanceQueryString = advancedQuery.getQuery();
	if(advanceQueryString != ""){
	queryString = advanceQueryString;
	}
}
	if (queryString == null)
		queryString = "";

	String htmlQueryString = Entities.encode(queryString);

	int start = 0; // first hit to display
	String startString = request.getParameter("start");
	if (startString != null)
		start = Integer.parseInt(startString);

	int hitsPerPage = 10; // number of hits to display
	String hitsString = request.getParameter("hitsPerPage");
	if (hitsString != null)
		hitsPerPage = Integer.parseInt(hitsString);
	if (MAX_HITS_PER_PAGE > 0 && hitsPerPage > MAX_HITS_PER_PAGE)
		hitsPerPage = MAX_HITS_PER_PAGE;

	int hitsPerSite = 0; // max hits per site
	String hitsPerSiteString = request.getParameter("hitsPerSite");
	if (hitsPerSiteString != null)
		hitsPerSite = Integer.parseInt(hitsPerSiteString);

	String sort = request.getParameter("sort");
	boolean reverse = sort != null
			&& "true".equals(request.getParameter("reverse"));

	String params = "&hitsPerPage="
			+ hitsPerPage
			+ (sort == null ? "" : "&sort=" + sort
					+ (reverse ? "&reverse=true" : ""));

	// get the lang from request
	String queryLang = request.getParameter("lang");
	if (queryLang == null) {
		queryLang = "zh-cn";
	}
	//Query query = Query.parse(queryString, queryLang, nutchConf);
	Query query = new Query(queryString);
	bean.LOG.info("query: " + queryString);
	bean.LOG.info("lang: " + queryLang);
	bean.LOG.info("parsed: " + query.toString());

	String language = ResourceBundle
			.getBundle("org.nutch.jsp.search", request.getLocale())
			.getLocale().getLanguage();
	String requestURI = HttpUtils.getRequestURL(request).toString();
	String base = requestURI.substring(0, requestURI.lastIndexOf('/'));
%>

<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<%
	// To prevent the character encoding declared with 'contentType' page
	// directive from being overriden by JSTL (apache i18n), we freeze it
	// by flushing the output buffer. 
	// see http://java.sun.com/developer/technicalArticles/Intl/MultilingualJSP/
	out.flush();
%>
<%@ taglib uri="http://jakarta.apache.org/taglibs/i18n" prefix="i18n"%>
<i18n:bundle baseName="org.nutch.jsp.search" />
<html lang="<%=language%>">
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<head>
<title>Kidden: <i18n:message key="title" /></title>
<link rel="icon" href="img/favicon.ico" type="image/x-icon" />
<link rel="shortcut icon" href="img/favicon.ico" type="image/x-icon" />
<jsp:include page="style/style.html" />

<base href="<%=base + "/" + language%>/">
<script type="text/javascript"  src="../js/jquery.js"></script>
<script type="text/javascript" src="../js/suggest.js" >

</script>
</head>

<body onLoad="queryfocus();">
	<form name="search" action="../search.jsp" method=get>
				<div id = "search">
								<table cellSpacing=0 cellPadding=0 width=100% border=0>
												<tr>
																<td valign=bottom><a href="./search.html"><img
																								height=36 alt=Kidden src="../img/logo_small.gif"
																								width=126 border=0></a></td>
																<td>&nbsp;&nbsp;</td>
																<td valign=bottom width=100%>
																				<table cellSpacing=0 cellPadding=0 border=0>
																								<tr>
																												<td noWrap>
																													<input  type="text"  id="search-text"   name="query" size=41 value="<%=htmlQueryString%>"> 
																													<input type="hidden" name="hitsPerPage"	value="<%=hitsPerPage%>"> 
																													<input type="hidden" name="lang" value="<%=queryLang%>">
																													<input type="submit" id="submit" value="<i18n:message key="search"/>">
																												</td>
																												<td noWrap>
																																<div class=shelpfont>
																																				&nbsp;&nbsp;<a
																																								href="advanced_web.html"><i18n:message
																																												key="advanced" /></a>&nbsp;&nbsp;<a
																																								href=""><i18n:message
																																												key="help" /></a>&nbsp;&nbsp;
																																</div>
																												</td>
																								</tr>
																				</table>
																</td>
												</tr>
								</table>
				</div>
</form>

				<%
					// how many hits to retrieve? if clustering is on and available,
					// take "hitsToCluster", otherwise just get hitsPerPage
					int hitsToRetrieve = hitsPerPage;

					Hits hits;
					try {
						query.getParams().initFrom(start + hitsToRetrieve, hitsPerSite,
								"site", sort, reverse);
						hits = bean.search(query);
					} catch (IOException e) {
						hits = new Hits(0, new Hit[0]);
					}
					int end = (int) Math.min(hits.getLength(), start + hitsPerPage);
					int length = end - start;
					int realEnd = (int) Math.min(hits.getLength(), start
							+ hitsToRetrieve);
					
					Hit[] show = hits.getHits(start, realEnd - start);
					HitDetails[] details = bean.getDetails(show);
					Summary[] summaries = bean.getSummary(details, query);
					bean.LOG.info("total hits: " + hits.getTotal());
				%>

				<table width=100% border=0 class=stat>
								<tr>
												<td noWrap width=1%>&nbsp;<b><i18n:message
																								key="web" /></b>&nbsp;
												</td>
												<%
												if(end!=0)
												{
												%>
												<td noWrap align=right><i18n:message key="hits">
																				<i18n:messageArg
																								value="<%=new Long((end==0)?0:(start + 1))%>" />
																				<i18n:messageArg value="<%=new Long(end)%>" />
																				<i18n:messageArg
																								value="<%=new Long(hits.getTotal())%>" />
																</i18n:message></td>
																<%
												}else{
													%>
													<td noWrap align=right><i18n:message key="noresult"></i18n:message>
													</td>
													<%
												}
																%>
																
								</tr>
				</table>

				<%
					// be responsive
					out.flush();
				%>

				<br>

				<%
				if(length==0){
					%>
					<div class="mnr-c">
					<div class="med card-section">  
					<p style="padding-top:.33em"> 
					<i18n:message key="noResultTips1">
					 <i18n:messageArg value="<%=htmlQueryString%>"/>
					</i18n:message>
					 </p> 
					 <p style="margin-top:1em">
					 <i18n:message key="noResultTips2"/>
					 </p> 
					 <ul style="margin-left:1.3em;margin-bottom:2em">
					 <i18n:message key="noResultTips3"/>
					 </ul>
					  </div> 
					  </div>
					<% 
				}
					for (int i = 0; i < length; i++) {
						Hit hit = show[i];
						HitDetails detail = details[i];
						String title = detail.getValue("title");
						String url = detail.getValue("url");
						
						String	TitleHigh = bean.getHighlightTitle(detail, query);
						if (title == null || title.equals(""))
							title = url;
						//title = bean.focusText(title, query, "zh-cn");

						String id = "idx=" + hit.getIndexNo() + "&id="
								+ hit.getUniqueKey();
						String summary = summaries[i].toString();//[i].toHtml(true);
						String caching = detail.getValue("cache");
						boolean showSummary = true;
						boolean showCached = true;
						if (caching != null) {
							showSummary = !caching.equals(Nutch.CACHING_FORBIDDEN_ALL);
							showCached = !caching.equals(Nutch.CACHING_FORBIDDEN_NONE);
						}

						//String primaryType = detail.getValue("primaryType");
						//String subType = detail.getValue("subType");
						//String type = primaryType + "/" + subType;
						boolean isText = false;
						//if ("text".equalsIgnoreCase(primaryType)
						//		|| type.equalsIgnoreCase("application/xhtml+xml"))
						//	isText = true;
						
						//String types =  detail.getValue("type");
						TypeUtils typeUtils = new TypeUtils();
						String[] types =  detail.getValues("type");

						HashSet<String> hashSet = new HashSet<String>();
						for(String type : types) {
							hashSet.add(type);
						}
						isText = typeUtils.isText(hashSet);

						//if(types!=null && (types.contains("text") ||types.contains("application/xhtml+xml")))
						 // isText = true;


						String shortType = "";
						if (!isText && hashSet!=null){

							shortType = typeUtils.getShortType(hashSet);
						}
						//	shortType = TypeUtils.getShortType(type);
				%>
				<%
					if (shortType.length() > 0) {
				%>
				[<%=shortType%>]
				<%
					}
				%>
				<a href="<%=url%>"><%=TitleHigh%></a>

				<%
					if (!"".equals(summary) && showSummary) {
				%>
				<br><%=summary%>
				<%
					}
				%>
				<br>
				<span class="url"><%=Entities.encode(url)%></span>
				<%
					if (showCached && isText) {
				%>
				(
				<a
								href="../cached.jsp?<%=id%>&queryString=<%=URLEncoder.encode(queryString, "UTF-8")%>&queryLang=<%=queryLang%>"
								target="_blank"><i18n:message key="cached" /></a>)
				<%
					}
				%>
				
				<%
          if (!isText) {
        %>
        <font size=-1><nobr>
                        - <a
                                href="../text.jsp?<%=id%>&queryString=<%=URLEncoder.encode(queryString, "UTF-8")%>&queryLang=<%=queryLang%>"
                                target="_blank"><i18n:message key="viewAsText" /></a>
                </nobr></font>
        <%
          }
        %>

				<%
					if (hit.moreFromDupExcluded()) {
							String more = "query="
									+ URLEncoder.encode("site:" + hit.getDedupValue()
											+ " " + queryString, "UTF8") + params
									+ "&hitsPerSite=" + 0 + "&lang=" + queryLang;
				%>
				(
				<a href="../search.jsp?<%=more%>"><i18n:message key="moreFrom" /><%=hit.getDedupValue()%></a>)
				<%
					}
				%>
				<br>
				<br>
				<%
					}
				%>

				<br>

				<%
					long n = hits.getTotal();
					int hp = hitsPerPage;
					int oneOrZero = (n % hp == 0 ? 0 : 1);
					long pagesTotal = (n <= 0) ? 0 : (n / hp + oneOrZero);
					long pagesNow = start / hp + 1;

					StringBuilder buf = new StringBuilder();

					if (pagesNow < 9) {
						for (int i = 1; i <= 15 && i <= pagesTotal; i++) {
							if (i == pagesNow)
								buf.append("<b>" + i + "</b>&nbsp");
							else
								buf.append("<a href=\"../search.jsp?query="
										+ URLEncoder.encode(queryString, "UTF-8") + "&start=" + ((i - 1) * hp)
										+ "&hitsPerPage=" + hitsPerPage
										+ "&hitsPerSite=" + hitsPerSite + "&lang="
										+ queryLang + "\">" + i + "</a>&nbsp");
						}
					} else {
						for (int i = 1; i < 8; i++) {
							buf.append("<a href=\"../search.jsp?query=" + URLEncoder.encode(queryString, "UTF-8")
									+ "&start=" + ((pagesNow - 9 + i) * hp)
									+ "&hitsPerPage=" + hitsPerPage + "&hitsPerSite="
									+ hitsPerSite + "&lang=" + queryLang + "\">"
									+ (pagesNow - 8 + i) + "</a>&nbsp");
						}

						buf.append("<b>" + pagesNow + "</b>&nbsp");

						for (int i = 1; i < 8 && i + pagesNow <= pagesTotal; i++) {
							buf.append("<a href=\"../search.jsp?query=" + URLEncoder.encode(queryString, "UTF-8")
									+ "&start=" + (pagesNow + i - 1) * hp
									+ "&hitsPerPage=" + hitsPerPage + "&hitsPerSite="
									+ hitsPerSite + "&lang=" + queryLang + "\">"
									+ (pagesNow + i) + "</a>&nbsp");
						}
					}

					String navigate = buf.toString();

					long prev = start - hitsPerPage;
					long next = pagesNow * hp;
				%>

				<div>
								<%
									if (pagesNow > 1) {
								%>
								<a
												href="../search.jsp?query=<%=URLEncoder.encode(queryString, "UTF-8")%>&start=<%=prev%>&hitsPerPage=<%=hitsPerPage %>&lang=<%=queryLang%>"><i18n:message
																key="previous" /></a>&nbsp;
								<%
									}
								%>

								<%=navigate%>

								<%
									if (pagesNow < pagesTotal) {
								%>
								<a
												href="../search.jsp?query=<%=URLEncoder.encode(queryString, "UTF-8")%>&start=<%=next%>&hitsPerPage=<%=hitsPerPage %>&lang=<%=queryLang%>"><i18n:message
																key="next" /></a>&nbsp;
								<%
									}
								%>
				</div>

				<%
					if ((!hits.totalIsExact() && (hits.getLength() <= start
							+ hitsPerPage))) {
				%>
				<form name="showAllHits" action="../search.jsp" method="get">
								<input type="hidden" name="query" value="<%=htmlQueryString%>">
								<input type="hidden" name="lang" value="<%=queryLang%>">
								<input type="hidden" name="hitsPerPage" value="<%=hitsPerPage%>">
								<input type="hidden" name="hitsPerSite" value="0"> <input
												type="submit" value="<i18n:message key="showAllHits"/>">
								<%
									if (sort != null) {
								%>
								<input type="hidden" name="sort" value="<%=sort%>"> <input
												type="hidden" name="reverse" value="<%=reverse%>">
								<%
									}
								%>
				</form>
				<%
					}
				%>

				<br>

				<table>
								<form name="search" action="../search.jsp" method=get>
												<tr>
																<td nowrap><input name="query" size=41
																				value="<%=htmlQueryString%>"> <input
																				type="hidden" name="hitsPerPage"
																				value="<%=hitsPerPage%>"> <input
																				type="hidden" name="lang" value="<%=queryLang%>">
																				<input type="submit"
																				value="<i18n:message key="search"/>"></td>
																<td noWrap>
																				<div class=shelpfont>
																								&nbsp;&nbsp;<a href="advanced_web.html"><i18n:message
																																key="advanced" /></a>&nbsp;&nbsp;<a href=""><i18n:message
																																key="help" /></a>&nbsp;&nbsp;
																				</div>
																</td>
												</tr>
								</form>
				</table>

</body>
</html>