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
package zipkin2.storage.kafka.index;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.GroupingSearch;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.Annotation;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;

import static zipkin2.storage.kafka.index.SpanIndexService.SpanFields.ANNOTATION;
import static zipkin2.storage.kafka.index.SpanIndexService.SpanFields.DURATION;
import static zipkin2.storage.kafka.index.SpanIndexService.SpanFields.ID;
import static zipkin2.storage.kafka.index.SpanIndexService.SpanFields.LOCAL_SERVICE_NAME;
import static zipkin2.storage.kafka.index.SpanIndexService.SpanFields.NAME;
import static zipkin2.storage.kafka.index.SpanIndexService.SpanFields.SORTED_TIMESTAMP;
import static zipkin2.storage.kafka.index.SpanIndexService.SpanFields.SORTED_TRACE_ID;
import static zipkin2.storage.kafka.index.SpanIndexService.SpanFields.TIMESTAMP;
import static zipkin2.storage.kafka.index.SpanIndexService.SpanFields.TRACE_ID;

public class SpanIndexService {
  static final Logger LOG = LoggerFactory.getLogger(SpanIndexService.class);

  final Directory directory;

  volatile IndexWriter indexWriter;

  SpanIndexService(Builder builder) throws IOException {
    LOG.info("Storing index on path={}", builder.indexDirectory);
    directory = new MMapDirectory(Paths.get(builder.indexDirectory));
    getIndexWriter();
  }

  public static SpanIndexService create(String indexDirectory) throws IOException {
    return new Builder().indexDirectory(indexDirectory).build();
  }

  IndexWriter getIndexWriter() {
    if (indexWriter == null) {
      synchronized (this) {
        if (indexWriter == null) {
          try {
            StandardAnalyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig indexWriterConfigs = new IndexWriterConfig(analyzer);
            indexWriter = new IndexWriter(directory, indexWriterConfigs);
            indexWriter.commit();
          } catch (Exception e) {
            LOG.error("Error opening index writer", e);
          }
        }
      }
    }
    return indexWriter;
  }

  public void deleteByTraceId(String traceId) {
    try {
      TermQuery query = new TermQuery(new Term(TRACE_ID, traceId));
      IndexWriter indexWriter = getIndexWriter();
      indexWriter.deleteDocuments(query);
      indexWriter.commit();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public Set<String> getTraceIds(QueryRequest queryRequest) {
    // Parsing query
    Query query = parseQuery(queryRequest);
    GroupingSearch groupingSearch = parseGrouping();
    try (IndexReader reader = DirectoryReader.open(directory)) {
      IndexSearcher indexSearcher = new IndexSearcher(reader);
      TopGroups<BytesRef> search =
          groupingSearch.search(indexSearcher, query, 0, queryRequest.limit());
      // Collecting trace ids
      Set<String> traceIds = new HashSet<>();
      for (GroupDocs<BytesRef> groupDocs : search.groups) {
        for (ScoreDoc scoreDoc : groupDocs.scoreDocs) {
          Document document = indexSearcher.doc(scoreDoc.doc);
          String traceId = document.get(TRACE_ID);
          traceIds.add(traceId);
        }
      }
      return traceIds;
    } catch (IOException e) {
      LOG.error("Error in group query", e);
      return new HashSet<>();
    }
  }

  public void insert(Span span) {
    try {
      Document doc = new Document();
      doc.add(
          new SortedDocValuesField(SORTED_TRACE_ID, new BytesRef(span.traceId())));
      doc.add(new NumericDocValuesField(SORTED_TIMESTAMP, span.timestampAsLong()));

      doc.add(new StringField(TRACE_ID, span.traceId(), Field.Store.YES));
      doc.add(new StringField(ID, span.id(), Field.Store.YES));

      String localServiceName =
          span.localServiceName() != null ? span.localServiceName() : "";
      doc.add(
          new StringField(LOCAL_SERVICE_NAME, localServiceName, Field.Store.YES));

      String name = span.name() != null ? span.name() : "";
      doc.add(new StringField(NAME, name, Field.Store.YES));

      doc.add(new LongPoint(TIMESTAMP, span.timestampAsLong()));
      doc.add(new LongPoint(DURATION, span.durationAsLong()));

      for (Map.Entry<String, String> tag : span.tags().entrySet()) {
        doc.add(new TextField(ANNOTATION, tag.getKey() + "=" + tag.getValue(),
            Field.Store.YES));
      }

      for (Annotation annotation : span.annotations()) {
        doc.add(new TextField(ANNOTATION, annotation.value(), Field.Store.YES));
      }

      IndexWriter indexWriter = getIndexWriter();
      indexWriter.addDocument(doc);
      indexWriter.commit();
    } catch (Exception e) {
      LOG.error("Error indexing span {}", span, e);
    }
  }

  GroupingSearch parseGrouping() {
    GroupingSearch groupingSearch = new GroupingSearch(SORTED_TRACE_ID);
    Sort sort = new Sort(new SortField(SORTED_TIMESTAMP, SortField.Type.LONG, true));
    groupingSearch.setGroupDocsLimit(1);
    groupingSearch.setGroupSort(sort);
    return groupingSearch;
  }

  Query parseQuery(QueryRequest queryRequest) {
    BooleanQuery.Builder builder = new BooleanQuery.Builder();

    if (queryRequest.serviceName() != null) {
      String serviceName = queryRequest.serviceName();
      TermQuery serviceNameQuery = new TermQuery(new Term(LOCAL_SERVICE_NAME, serviceName));
      builder.add(serviceNameQuery, BooleanClause.Occur.MUST);
    }

    if (queryRequest.spanName() != null) {
      String spanName = queryRequest.spanName();
      TermQuery spanNameQuery = new TermQuery(new Term(NAME, spanName));
      builder.add(spanNameQuery, BooleanClause.Occur.MUST);
    }

    if (queryRequest.annotationQueryString() != null) {
      try {
        QueryParser queryParser = new QueryParser(ANNOTATION, new StandardAnalyzer());
        Query annotationQuery = queryParser.parse(queryRequest.annotationQueryString());
        builder.add(annotationQuery, BooleanClause.Occur.MUST);
      } catch (ParseException e) {
        e.printStackTrace();
      }
    }

    if (queryRequest.maxDuration() != null) {
      Query durationRangeQuery = LongPoint.newRangeQuery(
          DURATION, queryRequest.minDuration(), queryRequest.maxDuration());
      builder.add(durationRangeQuery, BooleanClause.Occur.MUST);
    }

    long start = queryRequest.endTs() - queryRequest.lookback();
    long end = queryRequest.endTs();
    long lowerValue = start * 1000;
    long upperValue = end * 1000;
    Query tsRangeQuery = LongPoint.newRangeQuery(TIMESTAMP, lowerValue, upperValue);
    builder.add(tsRangeQuery, BooleanClause.Occur.MUST);

    return builder.build();
  }

  static class Builder {
    String indexDirectory;

    SpanIndexService build() throws IOException {
      return new SpanIndexService(this);
    }

    public Builder indexDirectory(String indexDirectory) {
      if (indexDirectory == null) throw new NullPointerException("indexDirectory == null");
      this.indexDirectory = indexDirectory;
      return this;
    }
  }

  static class SpanFields {
    static final String TRACE_ID = "trace_id";
    static final String SORTED_TRACE_ID = "trace_id_sorted";
    static final String ID = "id";
    static final String LOCAL_SERVICE_NAME = "local_service_name";
    static final String NAME = "name";
    static final String ANNOTATION = "annotation";
    static final String TIMESTAMP = "ts";
    static final String SORTED_TIMESTAMP = "ts_sorted";
    static final String DURATION = "duration";
  }
}
