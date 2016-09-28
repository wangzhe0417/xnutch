<%@ page language="java" import="java.util.*" pageEncoding="utf-8"
	import="org.apache.nutch.util.JsonData"
	import="org.apache.nutch.util.NullSuggest"
	%>
<%
	request.setCharacterEncoding("UTF-8");
	String searchText = request.getParameter("search-text");
	NullSuggest nullSuggest = new NullSuggest();
	List<String> words  = nullSuggest.getWords(searchText, 10);
	JsonData jsonData = new JsonData();
	String json = jsonData.getJson(words);
	out.println(json);

%>
