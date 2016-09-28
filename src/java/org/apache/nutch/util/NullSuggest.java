package org.apache.nutch.util;

import java.util.ArrayList;
import java.util.List;

public class NullSuggest   implements SuggestWord{
  
  public List<String> getWords(String searchText,int k){
   List<String>  words = new ArrayList<String>();
   return words;
  }

}
