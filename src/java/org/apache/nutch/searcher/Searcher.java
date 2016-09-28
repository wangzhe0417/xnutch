

package org.apache.nutch.searcher;

import java.io.IOException;

/** Service that searches. */
public interface Searcher extends java.io.Closeable {

  Hits search(Query query) throws IOException;

}
