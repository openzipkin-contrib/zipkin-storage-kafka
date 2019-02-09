/*
 * Copyright 2019 [name of copyright owner]
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
package zipkin2.storage.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.SpanStore;
import zipkin2.storage.kafka.internal.stores.IndexStateStore;
import zipkin2.storage.kafka.internal.stores.IndexStoreType;

public class KafkaSpanStore implements SpanStore {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaSpanStore.class);

  final String tracesStoreName;
  final String serviceStoreName;
  final String dependenciesStoreName;
  final String indexStoreName;
  final KafkaStreams processStreams;
  final KafkaStreams indexStreams;

  KafkaSpanStore(KafkaStorage storage) {
    tracesStoreName = storage.tracesTopic.name;
    serviceStoreName = storage.servicesTopic.name;
    dependenciesStoreName = storage.dependenciesTopic.name;
    indexStoreName = storage.indexStoreName;
    processStreams = storage.processStreams;
    indexStreams = storage.indexStreams;
  }

  @Override
  public Call<List<List<Span>>> getTraces(QueryRequest request) {
    ReadOnlyKeyValueStore<String, List<Span>> traceStore =
        processStreams.store(tracesStoreName, QueryableStoreTypes.keyValueStore());
    IndexStateStore indexStateStore = indexStreams.store(indexStoreName, new IndexStoreType());
    return new GetTracesCall(indexStateStore, request, traceStore);
  }

  static class GetTracesCall extends Call.Base<List<List<Span>>> {
    final IndexStateStore indexStateStore;
    final QueryRequest queryRequest;
    final ReadOnlyKeyValueStore<String, List<Span>> traceStore;

    GetTracesCall(IndexStateStore indexStateStore,
        QueryRequest queryRequest,
        ReadOnlyKeyValueStore<String, List<Span>> traceStore) {
      this.indexStateStore = indexStateStore;
      this.queryRequest = queryRequest;
      this.traceStore = traceStore;
    }

    @Override
    protected List<List<Span>> doExecute() throws IOException {
      return query();
    }

    private List<List<Span>> query() throws IOException {
      Directory directory = indexStateStore.directory();
      IndexReader reader = DirectoryReader.open(directory);
      IndexSearcher indexSearcher = new IndexSearcher(reader);

      BooleanQuery.Builder builder = new BooleanQuery.Builder();

      if (queryRequest.serviceName() != null) {
        String serviceName = queryRequest.serviceName();
        TermQuery serviceNameQuery = new TermQuery(new Term("local_service_name", serviceName));
        builder.add(serviceNameQuery, BooleanClause.Occur.MUST);
      }

      if (queryRequest.spanName() != null) {
        String spanName = queryRequest.spanName();
        TermQuery spanNameQuery = new TermQuery(new Term("name", spanName));
        builder.add(spanNameQuery, BooleanClause.Occur.MUST);
      }

      for (Map.Entry<String, String> entry : queryRequest.annotationQuery().entrySet()) {
        TermQuery spanNameQuery = new TermQuery(new Term(entry.getKey(), entry.getValue()));
        builder.add(spanNameQuery, BooleanClause.Occur.MUST);
      }

      if (queryRequest.maxDuration() != null) {
        builder.add(LongPoint.newRangeQuery(
            "duration",
            queryRequest.minDuration(),
            queryRequest.maxDuration()),
            BooleanClause.Occur.MUST);
      }

      long start = queryRequest.endTs() - queryRequest.lookback();
      long end = queryRequest.endTs();
      //TODO No timestamp field in Lucene. Find a way to query timestamp instead of longs.
      long lowerValue = Long.parseLong(start + "000");
      long upperValue = Long.parseLong(end + "000");
      builder.add(LongPoint.newRangeQuery("ts", lowerValue, upperValue), BooleanClause.Occur.MUST);

      int total = queryRequest.limit();
      Sort sort = Sort.RELEVANCE;

      Set<String> traceIds = new HashSet<>();

      BooleanQuery query = builder.build();
      TopFieldDocs docs = indexSearcher.search(query, total, sort);

      LOG.info("Total results of query {}: {}", query, docs.totalHits);

      for (ScoreDoc doc : docs.scoreDocs) {
        Document document = indexSearcher.doc(doc.doc);
        String traceId = document.get("trace_id");
        traceIds.add(traceId);
      }

      reader.close();

      return traceIds.stream()
          .map(traceStore::get)
          .collect(Collectors.toList());
    }

    @Override
    protected void doEnqueue(Callback<List<List<Span>>> callback) {
      try {
        callback.onSuccess(query());
      } catch (Exception e) {
        callback.onError(e);
      }
    }

    @Override
    public Call<List<List<Span>>> clone() {
      return new GetTracesCall(indexStateStore, queryRequest, traceStore);
    }
  }

  @Override
  public Call<List<Span>> getTrace(String traceId) {
    return new GetTraceCall(processStreams, tracesStoreName, traceId);
  }

  static class GetTraceCall extends KafkaStreamsStoreCall<List<Span>> {
    final KafkaStreams kafkaStreams;
    final String storeName;
    final String traceId;

    GetTraceCall(KafkaStreams kafkaStreams, String storeName, String traceId) {
      this.kafkaStreams = kafkaStreams;
      this.storeName = storeName;
      this.traceId = traceId;
    }

    @Override
    Supplier<List<Span>> query() {
      return () -> {
        try {
          ReadOnlyKeyValueStore<String, List<Span>> traceStore =
              kafkaStreams.store(storeName, QueryableStoreTypes.keyValueStore());
          return traceStore.get(traceId);
        } catch (Exception e) {
          LOG.error("Error getting trace with ID {}", traceId, e);
          return null;
        }
      };
    }

    @Override
    public Call<List<Span>> clone() {
      return new GetTraceCall(kafkaStreams, storeName, traceId);
    }
  }

  @Override
  public Call<List<String>> getServiceNames() {
    return new GetServiceNamesCall(processStreams, serviceStoreName);
  }

  static class GetServiceNamesCall extends KafkaStreamsStoreCall<List<String>> {
    final KafkaStreams kafkaStreams;
    final String storeName;

    GetServiceNamesCall(KafkaStreams kafkaStreams, String storeName) {
      this.kafkaStreams = kafkaStreams;
      this.storeName = storeName;
    }

    @Override
    Supplier<List<String>> query() {
      return () -> {
        try {
          ReadOnlyKeyValueStore<String, Set<String>> store =
              kafkaStreams.store(storeName, QueryableStoreTypes.keyValueStore());
          List<String> keys = new ArrayList<>();
          store.all().forEachRemaining(keyValue -> keys.add(keyValue.key));
          return keys;
        } catch (Exception e) {
          LOG.error("Error looking up services", e);
          return new ArrayList<>();
        }
      };
    }

    @Override
    public Call<List<String>> clone() {
      return new GetServiceNamesCall(kafkaStreams, storeName);
    }
  }

  @Override
  public Call<List<String>> getSpanNames(String serviceName) {
    return new GetSpanNamesCall(processStreams, serviceStoreName, serviceName);
  }

  static class GetSpanNamesCall extends KafkaStreamsStoreCall<List<String>> {
    final KafkaStreams kafkaStreams;
    final String storeName;
    final String serviceName;

    GetSpanNamesCall(KafkaStreams kafkaStreams, String storeName, String serviceName) {
      this.kafkaStreams = kafkaStreams;
      this.storeName = storeName;
      this.serviceName = serviceName;
    }

    @Override
    Supplier<List<String>> query() {
      return () -> {
        try {
          ReadOnlyKeyValueStore<String, Set<String>> store =
              kafkaStreams.store(storeName, QueryableStoreTypes.keyValueStore());
          if (serviceName == null || serviceName.equals("all")) return new ArrayList<>();
          Set<String> spanNames = store.get(serviceName);
          return new ArrayList<>(spanNames);
        } catch (Exception e) {
          LOG.error("Error looking up for span names for service {}", serviceName, e);
          return new ArrayList<>();
        }
      };
    }

    @Override
    public Call<List<String>> clone() {
      return new GetSpanNamesCall(kafkaStreams, storeName, serviceName);
    }
  }

  @Override
  public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
    return new GetDependenciesCall(processStreams, dependenciesStoreName);
  }

  static class GetDependenciesCall
      extends KafkaStreamsStoreCall<List<DependencyLink>> {
    final KafkaStreams kafkaStreams;
    final String storeName;

    GetDependenciesCall(KafkaStreams kafkaStreams, String storeName) {
      this.kafkaStreams = kafkaStreams;
      this.storeName = storeName;
    }

    @Override
    Supplier<List<DependencyLink>> query() {
      return () -> {
        try {
          ReadOnlyKeyValueStore<String, DependencyLink> store =
              kafkaStreams.store(storeName, QueryableStoreTypes.keyValueStore());
          List<DependencyLink> dependencyLinks = new ArrayList<>();
          store.all().forEachRemaining(dependencyLink -> dependencyLinks.add(dependencyLink.value));
          return dependencyLinks;
        } catch (Exception e) {
          LOG.error("Error looking up for dependencies", e);
          return new ArrayList<>();
        }
      };
    }

    @Override
    public Call<List<DependencyLink>> clone() {
      return new GetDependenciesCall(kafkaStreams, storeName);
    }
  }
}
