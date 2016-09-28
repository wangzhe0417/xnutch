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
package org.apache.nutch.indexwriter.lucene;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.nutch.analysis.AnalyzerFactory;
import org.apache.nutch.analysis.NutchAnalyzer;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.indexer.IndexingJob;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import java.util.*;
import java.util.Map.Entry;


public class LuceneWriter implements IndexWriter {

  private AnalyzerFactory analyzerFactory;

  private Configuration config;

  public static Logger LOG = LoggerFactory.getLogger(LuceneWriter.class);

  public static enum STORE {
    YES, NO
  }

  public static enum INDEX {
    NO, NO_NORMS, TOKENIZED, UNTOKENIZED
  }

  public static enum VECTOR {
    NO, OFFSET, POS, POS_OFFSET, YES
  }

  private final Map<String, FieldType> mapField;

  private static int deleteDoc;

  @Override
  public void setConf(Configuration configuration) {
    this.config = configuration;

  }

  @Override
  public Configuration getConf() {
    return config;
  }

  private org.apache.lucene.index.IndexWriter writer;

  private Path perm;

  private Path temp;

  private FileSystem fs;

  public LuceneWriter() {
    mapField = new HashMap<String, FieldType>();
    deleteDoc = 0;
  }

  private Document createLuceneDoc(NutchDocument doc) throws IOException {

    final Document out = new Document();
    for (final Entry<String, NutchField> entry : doc) {
      final String fieldName = entry.getKey();
      FieldType fieldType = mapField.get(fieldName);
      if (fieldType == null) {
        throw new IOException("filename: " + fieldName + "  not found");
      }

      for (final Object fieldValue : entry.getValue().getValues()) {

        Field field = new Field(fieldName, fieldValue.toString(), fieldType);

        out.add(field);
      }
    }
    return out;
  }

  private void processOptions(Configuration conf) {
    final Iterator<Entry<String, String>> iterator = conf.iterator();
    
    while (iterator.hasNext()) {

      final String key =  ((Map.Entry<String, String>) iterator.next()).getKey();
      if (!key.startsWith(LuceneConstants.LUCENE_PREFIX)) {
        continue;
      }
      // System.out.println(key);
      if (key.startsWith(LuceneConstants.FIELD_STORE_PREFIX)) {
        final String field = key
            .substring(LuceneConstants.FIELD_STORE_PREFIX.length());
        final LuceneWriter.STORE store = LuceneWriter.STORE
            .valueOf(conf.get(key));
        // fieldStore = conf.get(key);
        switch (store) {
        case YES:
          if (mapField.containsKey(field)) {
            FieldType fieldType = mapField.get(field);
            fieldType.setStored(true);
            mapField.put(field, fieldType);
          } else {
            FieldType fieldType = new FieldType();
            fieldType.setStored(true);
            mapField.put(field, fieldType);
          }
          break;
        case NO:
          if (mapField.containsKey(field)) {
            FieldType fieldType = mapField.get(field);
            fieldType.setStored(false);
            mapField.put(field, fieldType);
          } else {
            FieldType fieldType = new FieldType();
            fieldType.setStored(false);
            mapField.put(field, fieldType);
          }
          break;
        }
      } else if (key.startsWith(LuceneConstants.FIELD_INDEX_PREFIX)) {
        final String field = key
            .substring(LuceneConstants.FIELD_INDEX_PREFIX.length());
        final LuceneWriter.INDEX index = LuceneWriter.INDEX
            .valueOf(conf.get(key));
        switch (index) {
        case NO:
          if (mapField.containsKey(field)) {
            FieldType fieldType = mapField.get(field);
            fieldType.setIndexed(false);
            mapField.put(field, fieldType);
          } else {
            FieldType fieldType = new FieldType();
            fieldType.setIndexed(false);
            mapField.put(field, fieldType);
          }
          break;
        case NO_NORMS:
          if (mapField.containsKey(field)) {
            FieldType fieldType = mapField.get(field);
            fieldType.setIndexed(true);
            fieldType.setOmitNorms(false);
            mapField.put(field, fieldType);
          } else {
            FieldType fieldType = new FieldType();
            fieldType.setIndexed(true);
            fieldType.setOmitNorms(false);
            mapField.put(field, fieldType);
          }
          break;
        case TOKENIZED:
          if (mapField.containsKey(field)) {
            FieldType fieldType = mapField.get(field);
            fieldType.setIndexed(true);
            fieldType.setTokenized(true);
            mapField.put(field, fieldType);
          } else {
            FieldType fieldType = new FieldType();
            fieldType.setIndexed(true);
            fieldType.setTokenized(true);
            mapField.put(field, fieldType);
          }
          break;
        case UNTOKENIZED:
          if (mapField.containsKey(field)) {
            FieldType fieldType = mapField.get(field);
            fieldType.setIndexed(true);
            fieldType.setTokenized(false);
            mapField.put(field, fieldType);
          } else {
            FieldType fieldType = new FieldType();
            fieldType.setIndexed(true);
            fieldType.setTokenized(false);
            mapField.put(field, fieldType);
          }
          break;
        }
      } else if (key.startsWith(LuceneConstants.FIELD_VECTOR_PREFIX)) {
        final String field = key
            .substring(LuceneConstants.FIELD_VECTOR_PREFIX.length());
        final LuceneWriter.VECTOR vector = LuceneWriter.VECTOR
            .valueOf(conf.get(key));
        switch (vector) {
        case NO:
          if (mapField.containsKey(field)) {
            FieldType fieldType = mapField.get(field);
            fieldType.setStoreTermVectors(false);
            mapField.put(field, fieldType);
          } else {
            FieldType fieldType = new FieldType();
            fieldType.setStoreTermVectors(false);
            mapField.put(field, fieldType);
          }
          break;
        case OFFSET:
          if (mapField.containsKey(field)) {
            FieldType fieldType = mapField.get(field);
            fieldType.setStoreTermVectors(true);
            fieldType.setStoreTermVectorOffsets(true);
            mapField.put(field, fieldType);
          } else {
            FieldType fieldType = new FieldType();
            fieldType.setStoreTermVectors(true);
            fieldType.setStoreTermVectorOffsets(true);
            mapField.put(field, fieldType);
          }
          break;
        case POS:
          if (mapField.containsKey(field)) {
            FieldType fieldType = mapField.get(field);
            fieldType.setStoreTermVectors(true);
            fieldType.setStoreTermVectorPositions(true);
            mapField.put(field, fieldType);
          } else {
            FieldType fieldType = new FieldType();
            fieldType.setStoreTermVectors(true);
            fieldType.setStoreTermVectorPositions(true);
            mapField.put(field, fieldType);
          }
          break;
        case POS_OFFSET:
          if (mapField.containsKey(field)) {
            FieldType fieldType = mapField.get(field);
            fieldType.setStoreTermVectors(true);
            fieldType.setStoreTermVectorPositions(true);
            fieldType.setStoreTermVectorOffsets(true);
            mapField.put(field, fieldType);
          } else {
            FieldType fieldType = new FieldType();
            fieldType.setStoreTermVectors(true);
            fieldType.setStoreTermVectorPositions(true);
            fieldType.setStoreTermVectorOffsets(true);
            mapField.put(field, fieldType);
          }
          break;
        case YES:
          if (mapField.containsKey(field)) {
            FieldType fieldType = mapField.get(field);
            fieldType.setStoreTermVectors(true);
            mapField.put(field, fieldType);
          } else {
            FieldType fieldType = new FieldType();
            fieldType.setStoreTermVectors(true);
            mapField.put(field, fieldType);
          }
          break;
        }
      }

    }

  }

  public void open(JobConf job, String name) throws IOException {
    this.fs = FileSystem.get(job);
    perm = new Path(FileOutputFormat.getOutputPath(job), name);
    temp = job
        .getLocalPath("index/_" + Integer.toString(new Random().nextInt()));
    fs.delete(perm, true); // delete old, if any
    analyzerFactory = new AnalyzerFactory(job);
    IndexWriterConfig indexWriterConfig = new IndexWriterConfig(
        Version.LUCENE_4_10_2, new SmartChineseAnalyzer());
    LogByteSizeMergePolicy mergePolicy = new LogByteSizeMergePolicy();
    mergePolicy.setMergeFactor(job.getInt("indexer.mergeFactor", 10));
    mergePolicy
        .setMaxMergeDocs(job.getInt("indexer.maxMergeDocs", Integer.MAX_VALUE));

    indexWriterConfig.setMergePolicy(mergePolicy);
    indexWriterConfig.setUseCompoundFile(false);
    indexWriterConfig
        .setTermIndexInterval(job.getInt("indexer.termIndexInterval", 128));
    indexWriterConfig
        .setMaxBufferedDocs(job.getInt("indexer.minMergeDocs", 100));
    indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
    writer = new org.apache.lucene.index.IndexWriter(
        FSDirectory.open(new File(fs.startLocalOutput(perm, temp).toString())),
        indexWriterConfig);

    /*
     * addFieldOptions("title", STORE.YES, INDEX.TOKENIZED, VECTOR.NO, job);
     * addFieldOptions("url", STORE.YES, INDEX.TOKENIZED, VECTOR.NO, job);
     * addFieldOptions("content", STORE.YES, INDEX.TOKENIZED, VECTOR.NO, job);
     * addFieldOptions("lang", STORE.YES, INDEX.UNTOKENIZED, VECTOR.NO, job);
     */

    processOptions(job);
  }

  public void close() throws IOException {
    /*删除你想要删除的文档*/
    //delete("http://music.163.com/");
    LOG.info("The Number Of DeleteDoc is : " + deleteDoc);
    writer.close();
    fs.completeLocalOutput(perm, temp); // copy to dfs
    fs.createNewFile(
        new Path(perm, org.apache.nutch.indexer.IndexingJob.DONE_NAME));
  }

  @Override
  public String describe() {
    StringBuilder sb = new StringBuilder("LuceneIndexWriter\n");
    sb.append("\t").append("Indexing Field:\n");
    for (String str : mapField.keySet()) {
      sb.append("\t").append(str).append("\t, store: \t")
          .append(mapField.get(str).stored()).append("\t , index: \t")
          .append(mapField.get(str).indexed()).append("\t, tokenized: \t")
          .append(mapField.get(str).tokenized()).append("\t, vector: \t")
          .append(mapField.get(str).storeTermVectors()).append("\n");
    }
    return sb.toString();
  }

  public void write(NutchDocument doc) throws IOException {
    final Document luceneDoc = createLuceneDoc(doc);

    //final NutchAnalyzer analyzer = analyzerFactory.getDefault();
    final NutchAnalyzer analyzer = analyzerFactory.get(luceneDoc.get("lang"));
    if (IndexingJob.LOG.isDebugEnabled()) {
      IndexingJob.LOG.debug("Indexing [" + luceneDoc.get("url")
              + "] with analyzer " + analyzer + " (" + luceneDoc.get("lang")
              + ")");
    }
    writer.addDocument(luceneDoc, analyzer);
  }

  @Override
  public void delete(String key) throws IOException {
    Term term = new Term("id", key);
    writer.deleteDocuments(term);
    deleteDoc++;
  }

  @Override
  public void update(NutchDocument doc) throws IOException {
    write(doc);
  }

  @Override
  public void commit() throws IOException {
    writer.commit();
  }

  public static void addFieldOptions(String field, LuceneWriter.STORE store,
      LuceneWriter.INDEX index, LuceneWriter.VECTOR vector,
      Configuration conf) {

    /* Only set the field options if none have been configured already */
    if (conf.get(LuceneConstants.FIELD_STORE_PREFIX + field) == null) {
      conf.set(LuceneConstants.FIELD_STORE_PREFIX + field, store.toString());
    }
    if (conf.get(LuceneConstants.FIELD_INDEX_PREFIX + field) == null) {
      conf.set(LuceneConstants.FIELD_INDEX_PREFIX + field, index.toString());
    }
    if (conf.get(LuceneConstants.FIELD_VECTOR_PREFIX + field) == null) {
      conf.set(LuceneConstants.FIELD_VECTOR_PREFIX + field, vector.toString());
    }

  }

  public static void addFieldOptions(String field, LuceneWriter.STORE store,
      LuceneWriter.INDEX index, Configuration conf) {
    LuceneWriter.addFieldOptions(field, store, index, LuceneWriter.VECTOR.NO,
        conf);
  }
}
