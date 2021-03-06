package org.apache.nutch.indexwriter.lucene;

/**
 * Created by zhangmingke on 16/7/15.
 */
public class LuceneConstants {
    public static final String LUCENE_PREFIX = "lucene.";

    public static final String FIELD_PREFIX = LUCENE_PREFIX + "field.";

    public static final String FIELD_STORE_PREFIX = FIELD_PREFIX + "store.";

    public static final String FIELD_INDEX_PREFIX = FIELD_PREFIX + "index.";

    public static final String FIELD_VECTOR_PREFIX = FIELD_PREFIX + "vector.";

    public static final String STORE_YES = "store.yes";

    public static final String STORE_NO = "store.no";

    public static final String STORE_COMPRESS = "store.compress";

    public static final String INDEX_NO = "index.no";

    // TODO: -> ANALYZED_NO_NORMS
    public static final String INDEX_NO_NORMS = "index.no_norms";

    // TODO: -> ANALYZED
    public static final String INDEX_TOKENIZED = "index.tokenized";

    // TODO: -> NOT_ANALYZED
    public static final String INDEX_UNTOKENIZED = "index.untokenized";

    public static final String VECTOR_NO = "vector.no";

    public static final String VECTOR_POS = "vector.pos";

    public static final String VECTOR_OFFSET = "vector.offset";

    public static final String VECTOR_POS_OFFSET = "vector.pos_offset";

    public static final String VECTOR_YES = "vector.yes";

}
