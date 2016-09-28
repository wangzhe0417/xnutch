package org.apache.nutch.util;
import java.util.List;

public class JsonData  {

  public String getJson(List<String> listWords){
    String json = "[";
    if (listWords.isEmpty()) {
      return null;
    }
    for (String word : listWords) {
      json += "\"" + word + "\",";
    }
    json = json.substring(0, json.length() - 1 > 0 ? json.length() - 1 : 1);
    json += "]";
    return json;
  }

}
