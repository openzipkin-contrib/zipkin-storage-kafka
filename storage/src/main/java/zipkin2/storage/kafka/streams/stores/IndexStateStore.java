/*
 * Copyright 2019 jeqo
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin2.storage.kafka.streams.stores;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.GroupingSearch;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexStateStore implements StateStore {

  private static final Logger LOG = LoggerFactory.getLogger(IndexStateStore.class);

  final String name;
  final String indexDirectory;

  volatile Directory directory;
  volatile IndexWriter indexWriter;

  IndexStateStore(Builder builder) {
    LOG.info("Storing index on path={}", builder.indexDirectory);
    name = builder.name();
    indexDirectory = builder.indexDirectory;
  }

  public static Builder builder(String name, String indexDirectory) {
    return new Builder(name, indexDirectory);
  }

  Directory getDirectory() {
    if (directory == null) {
      synchronized (this) {
        if (directory == null) {
          try {
            directory = new NIOFSDirectory(Paths.get(indexDirectory));
          } catch (IOException e) {
            LOG.error("Error creating lucene directory", e);
          }
        }
      }
    }
    return directory;
  }

  IndexWriter getIndexWriter() {
    if (indexWriter == null) {
      synchronized (this) {
        if (indexWriter == null) {
          try {
            StandardAnalyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig indexWriterConfigs = new IndexWriterConfig(analyzer);
            indexWriter = new IndexWriter(getDirectory(), indexWriterConfigs);
          } catch (Exception e) {
            LOG.error("Error opening index writer", e);
          }
        }
      }
    }
    return indexWriter;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public void init(ProcessorContext context, StateStore root) {
    try {
      context.register(root, (key, value) -> {
      });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void flush() {
    try {
      getIndexWriter().flush();
    } catch (IOException e) {
      LOG.error("Error flushing state", e);
    }
  }

  @Override
  public void close() {
    try {
      getIndexWriter().close();
    } catch (IOException e) {
      LOG.error("Error closing index writer", e);
    }
  }

  @Override
  public boolean persistent() {
    return true;
  }

  @Override
  public boolean isOpen() {
    return getIndexWriter().isOpen();
  }

  public void put(List<Document> value) {
    try {
      for (Document doc : value) {
        getIndexWriter().addDocument(doc);
      }
      getIndexWriter().commit();
      LOG.debug("{} indexed documents", value.size());
    } catch (IOException e) {
      LOG.error("Error indexing documents", e);
    }
  }

  public void delete(Query query) {
    try {
      getIndexWriter().deleteDocuments(query);
      getIndexWriter().commit();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public Document get(Query query) {
    try (IndexReader reader = DirectoryReader.open(getDirectory())) {
      IndexSearcher indexSearcher = new IndexSearcher(reader);
      TopDocs docs = indexSearcher.search(query, 1);
      if (docs.totalHits > 0) {
        return indexSearcher.doc(docs.scoreDocs[0].doc);
      }
      return null;
    } catch (IOException e) {
      LOG.error("Error in group query", e);
      return null;
    }
  }

  public List<Document> groupSearch(
      GroupingSearch groupingSearch,
      BooleanQuery query,
      int offset,
      int limit) {
    try (IndexReader reader = DirectoryReader.open(getDirectory())) {
      IndexSearcher indexSearcher = new IndexSearcher(reader);

      TopGroups<BytesRef> search = groupingSearch.search(indexSearcher, query, offset, limit);

      List<Document> documents = new ArrayList<>();

      for (GroupDocs<BytesRef> doc : search.groups) {
        for (ScoreDoc scoreDoc : doc.scoreDocs) {
          Document document = indexSearcher.doc(scoreDoc.doc);
          documents.add(document);
        }
      }

      return documents;
    } catch (IOException e) {
      LOG.error("Error in group query", e);
      return new ArrayList<>();
    }
  }

  public static class Builder implements StoreBuilder<IndexStateStore> {
    final String name;

    String indexDirectory;
    boolean loggingEnabled;

    Builder(String name, String indexDirectory) {
      this.name = name;
      this.indexDirectory = indexDirectory;
    }

    @Override
    public Builder withCachingEnabled() {
      return null;
    }

    @Override
    public Builder withCachingDisabled() {
      throw new UnsupportedOperationException("caching not supported");
    }

    @Override
    public Builder withLoggingDisabled() {
      loggingEnabled = false;
      return this;
    }

    @Override
    public IndexStateStore build() {
      return new IndexStateStore(this);
    }

    @Override
    public Map<String, String> logConfig() {
      return null;
    }

    @Override
    public boolean loggingEnabled() {
      return loggingEnabled;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public Builder withLoggingEnabled(Map config) {
      loggingEnabled = true;
      return this;
    }
  }
}
