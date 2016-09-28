package org.apache.nutch.searcher;

import org.apache.hadoop.io.WritableComparable;


public class SolrHit extends Hit {

  private HitDetails hitDetails;
  private Summary summary;
  
  private String titleHighlighted;
  
  public SolrHit(String uniqueKey, WritableComparable sortValue, String dedupValue){
    super(uniqueKey, sortValue, dedupValue);
  }
  
  public HitDetails getHitDetails() {
    return hitDetails;
  }
  public void setHitDetails(HitDetails hitDetails) {
    this.hitDetails = hitDetails;
  }
  public Summary getSummary() {
    return summary;
  }
  public void setSummary(Summary summary) {
    this.summary = summary;
  }

  public String getTitleHighlighted() {
    return titleHighlighted;
  }

  public void setTitleHighlighted(String titleHighlighted) {
    this.titleHighlighted = titleHighlighted;
  }
  
  
}
