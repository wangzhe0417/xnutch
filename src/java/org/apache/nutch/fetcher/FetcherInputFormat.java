package org.apache.nutch.fetcher;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.nutch.crawl.CrawlDatum;

public class FetcherInputFormat extends
    SequenceFileInputFormat<Text, CrawlDatum> {
  /** Don't split inputs, to keep things polite. */
  public InputSplit[] getSplits(JobConf job, int nSplits) throws IOException {
    FileStatus[] files = listStatus(job);
    FileSplit[] splits = new FileSplit[files.length];
    for (int i = 0; i < files.length; i++) {
      FileStatus cur = files[i];
      splits[i] = new FileSplit(cur.getPath(), 0, cur.getLen(),
          (String[]) null);
    }
    return splits;
  }
}