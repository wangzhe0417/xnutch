package org.apache.nutch.util;

import java.util.List;

public interface SuggestWord {
    
  public List<String> getWords(String searchstr,int k);//TopK word

}
