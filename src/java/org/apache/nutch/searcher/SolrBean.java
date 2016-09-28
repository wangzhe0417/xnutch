/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.searcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
//import org.apache.lucene.index.Term;
//import org.apache.lucene.search.BooleanClause;
//import org.apache.lucene.search.BooleanQuery;
//import org.apache.lucene.search.TermQuery;
//import org.apache.lucene.util.ToStringUtils;
//import org.apache.nutch.indexer.solr.SolrMappingReader;
//import org.apache.nutch.indexer.solr.SolrWriter;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

public class SolrBean implements Searcher, HitDetailer, HitSummarizer {

  public static final Log LOG = LogFactory.getLog(SolrBean.class);

  private final SolrServer solr;

//  private final QueryFilters filters;
  
  private String searchUID= "id";

  public SolrBean(Configuration conf, String solrServer)
  throws IOException {
    solr = new HttpSolrServer(solrServer);
//    filters = new QueryFilters(conf);
//    SolrMappingReader mapping = SolrMappingReader.getInstance(conf);
//    searchUID = mapping.getUniqueKey();
  }

  public String getExplanation(Query query, Hit hit) throws IOException {
    return "SOLR backend does not support explanations yet.";
  }

  public static IOException makeIOException(SolrServerException e) {
    final IOException ioe = new IOException();
    ioe.initCause(e);
    return ioe;
  }
  
  public Hits search(Query query) throws IOException {
    // filter query string
//    final BooleanQuery bQuery = filters.filter(query);

//    final SolrQuery solrQuery = new SolrQuery(stringify(bQuery));
    final SolrQuery solrQuery = new SolrQuery();
    
    solrQuery.set("q", query.getQuery());

    solrQuery.setRows(query.getParams().getNumHits());

    if (query.getParams().getSortField() == null) {
//      solrQuery.setFields(query.getParams().getDedupField(), "score", searchUID);
      query.getParams().setSortField("score");
    } else {
//      solrQuery.setFields(query.getParams().getDedupField(), query
//          .getParams().getSortField(), searchUID);
//      solrQuery.setSortField(query.getParams().getSortField(), query
//          .getParams().isReverse() ? ORDER.asc : ORDER.desc);

      solrQuery.setSort(query.getParams().getSortField(), query
              .getParams().isReverse() ? ORDER.asc : ORDER.desc);
    }

    solrQuery.set("fl", "id,url,title,tstamp,type,content,segment,score");
    solrQuery.setHighlight(true);
    solrQuery.set("hl.fl", "title,content");
    solrQuery.set("hl.simple.pre", "<span class=highlight>");
    solrQuery.set("hl.simple.post", "</span>");
    solrQuery.set("defType", "edismax");
    solrQuery.set("qf", "title^4 content");

    QueryResponse response;
    try {
      response = solr.query(solrQuery);
    } catch (final SolrServerException e) {
      throw makeIOException(e);
    }

    final SolrDocumentList docList = response.getResults();
    
    Map<String,Map<String,List<String>>> highlights = response.getHighlighting();
    
    int qtime = response.getQTime();

    final Hit[] hitArr = new Hit[docList.size()];
    for (int i = 0; i < hitArr.length; i++) {
      final SolrDocument solrDoc = docList.get(i);
      
      String url = (String) solrDoc.getFieldValue("url");
      String title = (String) solrDoc.getFieldValue("title");
      String content = (String) solrDoc.getFieldValue("content");

      final Object raw = solrDoc.getFirstValue(query.getParams().getSortField());
      WritableComparable sortValue;

      if (raw instanceof Integer) {
        sortValue = new IntWritable(((Integer)raw).intValue());
      } else if (raw instanceof Float) {
        sortValue = new FloatWritable(((Float)raw).floatValue());
      } else if (raw instanceof String) {
        sortValue = new Text((String)raw);
      } else if (raw instanceof Long) {
        sortValue = new LongWritable(((Long)raw).longValue());
      } else {
        throw new RuntimeException("Unknown sort value type!");
      }

      final String dedupValue = (String) solrDoc.getFirstValue(query.getParams().getDedupField());

      final String uniqueKey = (String )solrDoc.getFirstValue(searchUID);

//      hitArr[i] = new Hit(uniqueKey, sortValue, dedupValue);
      SolrHit hit = new SolrHit(uniqueKey, sortValue, dedupValue);  
      SolrHitDetails details = buildDetails(solrDoc);
      details.setHit(hit);
      hit.setHitDetails(details);
      
      hit.setTitleHighlighted(title);
      int len = (content.length()>100?100:content.length());
      Summary.Fragment f = new Summary.Fragment(content.substring(0, len));
      Summary summary = new Summary();
      summary.add(f);
      hit.setSummary(summary);
      
      String titleHighlighted = "";
      if(highlights.containsKey(url)){
        Map<String,List<String>> snippets = highlights.get(url);
        if(snippets.containsKey("title")){
          titleHighlighted = snippets.get("title").get(0);
          hit.setTitleHighlighted(titleHighlighted);
        }
        
        if(snippets.containsKey("content")){
          f = new Summary.Fragment(snippets.get("content").get(0));
          summary = new Summary();
          summary.add(f);
          hit.setSummary(summary);
        }
      }
      
      hitArr[i] = hit;
    }

    return new Hits(docList.getNumFound(), hitArr);
  }

  @SuppressWarnings("unchecked")
  @Deprecated
  public Hits search(Query query, int numHits, String dedupField,
                     String sortField, boolean reverse)
  throws IOException {
    query.getParams().setNumHits(numHits); 
    query.getParams().setDedupField(dedupField); 
    query.getParams().setSortField(sortField); 
    query.getParams().setReverse(reverse);
    return search(query);
  }

  public HitDetails getDetails(Hit hit) throws IOException {
//    SolrHit h = (SolrHit) hit;
//    return h.getHitDetails();
    QueryResponse response;
    try {
      String url = hit.getUniqueKey();
      url.replace(":", "\\:");
      SolrQuery solrQuery = new SolrQuery(searchUID + ":\"" + url + "\"");
      solrQuery.set("fl", "id,url,title,tstamp,type,content,segment,score");
      response = solr.query(solrQuery);
    } catch (final SolrServerException e) {
      throw makeIOException(e);
    }

    final SolrDocumentList docList = response.getResults();
    if (docList.getNumFound() == 0) {
      return null;
    }

    return buildDetails(docList.get(0));
  }

  public HitDetails[] getDetails(Hit[] hits) throws IOException {
    HitDetails[] details = new HitDetails[hits.length];
    for(int i=0; i<hits.length; i++){
      //details[i] = getDetails(hits[i]);
      SolrHit h = (SolrHit) hits[i];
      details[i] = h.getHitDetails();
    }
    return details;
    
//    final StringBuilder buf = new StringBuilder();
//    buf.append("(");
//    for (final Hit hit : hits) {
//      buf.append(" " + searchUID + ":\"");
//      buf.append(hit.getUniqueKey());
//      buf.append("\"");
//    }
//    buf.append(")");
//
//    QueryResponse response;
//    try {
//      response = solr.query(new SolrQuery(buf.toString()));
//    } catch (final SolrServerException e) {
//      throw makeIOException(e);
//    }
//
//    final SolrDocumentList docList = response.getResults();
//    if (docList.size() < hits.length) {
//      throw new RuntimeException("Missing hit details! Found: " +
//                                 docList.size() + ", expecting: " +
//                                 hits.length);
//    }
//
//    /* Response returned from SOLR server may be out of
//     * order. So we make sure that nth element of HitDetails[]
//     * is the detail of nth hit.
//     */
//    final Map<String, HitDetails> detailsMap =
//      new HashMap<String, HitDetails>(hits.length);
//    for (final SolrDocument solrDoc : docList) {
//      final HitDetails details = buildDetails(solrDoc);
//      detailsMap.put(details.getValue(searchUID), details);
//    }
//
//    final HitDetails[] detailsArr = new HitDetails[hits.length];
//    for (int i = 0; i < hits.length; i++) {
//      detailsArr[i] = detailsMap.get(hits[i].getUniqueKey());
//    }
//
//    return detailsArr;
  }

  public boolean ping() throws IOException {
    try {
      return solr.ping().getStatus() == 0;
    } catch (final SolrServerException e) {
      throw makeIOException(e);
    }
  }

  public void close() throws IOException { }

  private static SolrHitDetails buildDetails(SolrDocument solrDoc) {
    final List<String> fieldList = new ArrayList<String>();
    final List<String> valueList = new ArrayList<String>();
    for (final String field : solrDoc.getFieldNames()) {
      for (final Object o : solrDoc.getFieldValues(field)) {
        fieldList.add(field);
        valueList.add(o.toString());
      }
    }

    final String[] fields = fieldList.toArray(new String[fieldList.size()]);
    final String[] values = valueList.toArray(new String[valueList.size()]);
    return new SolrHitDetails(fields, values);
  }

//  /* Hackish solution for stringifying queries. Code from BooleanQuery.
//   * This is necessary because a BooleanQuery.toString produces
//   * statements like feed:http://www.google.com which doesn't work, we
//   * need feed:"http://www.google.com".
//   */
//  private static String stringify(BooleanQuery bQuery) {
//    final StringBuilder buffer = new StringBuilder();
//    final boolean needParens=(bQuery.getBoost() != 1.0) ||
//                       (bQuery.getMinimumNumberShouldMatch()>0) ;
//    if (needParens) {
//      buffer.append("(");
//    }
//
//    final BooleanClause[] clauses  = bQuery.getClauses();
//    int i = 0;
//    for (final BooleanClause c : clauses) {
//      if (c.isProhibited())
//        buffer.append("-");
//      else if (c.isRequired())
//        buffer.append("+");
//
//      final org.apache.lucene.search.Query subQuery = c.getQuery();
//      if (subQuery instanceof BooleanQuery) {   // wrap sub-bools in parens
//        buffer.append("(");
//        buffer.append(c.getQuery().toString(""));
//        buffer.append(")");
//      } else if (subQuery instanceof TermQuery) {
//        final Term term = ((TermQuery) subQuery).getTerm();
//        buffer.append(term.field());
//        buffer.append(":\"");
//        buffer.append(term.text());
//        buffer.append("\"");
//      } else {
//        buffer.append(" ");
//        buffer.append(c.getQuery().toString());
//      }
//
//      if (i++ != clauses.length - 1) {
//        buffer.append(" ");
//      }
//    }
//
//    if (needParens) {
//      buffer.append(")");
//    }
//
//    if (bQuery.getMinimumNumberShouldMatch()>0) {
//      buffer.append('~');
//      buffer.append(bQuery.getMinimumNumberShouldMatch());
//    }
//
//    if (bQuery.getBoost() != 1.0f) {
//      buffer.append(ToStringUtils.boost(bQuery.getBoost()));
//    }
//
//    return buffer.toString();
//  }
//
  @Override
  public Summary getSummary(HitDetails details, Query query) throws IOException {
    SolrHitDetails hd = (SolrHitDetails) details;
    return hd.getHit().getSummary();
  }
  
  public String getHighlightedTitle(HitDetails details, Query query){
	    SolrHitDetails hd = (SolrHitDetails) details;
	    return hd.getHit().getTitleHighlighted();
	  }
  
  
  @Override
  public Summary[] getSummary(HitDetails[] details, Query query)
      throws IOException {
    Summary[] summaries = new Summary[details.length];
    for(int i=0; i<details.length; i++)
      summaries[i] = getSummary(details[i], query);
    
    return summaries;
  }

}
