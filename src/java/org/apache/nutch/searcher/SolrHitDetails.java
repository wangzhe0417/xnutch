package org.apache.nutch.searcher;

public class SolrHitDetails extends HitDetails {
  
  private SolrHit hit;

  public SolrHit getHit() {
    return hit;
  }

  public void setHit(SolrHit hit) {
    this.hit = hit;
  }

  public SolrHitDetails() {
    super();
  }

  public SolrHitDetails(String segment, String url) {
    super(segment, url);
  }

  public SolrHitDetails(String[] fields, String[] values) {
    super(fields, values);
  }

}
