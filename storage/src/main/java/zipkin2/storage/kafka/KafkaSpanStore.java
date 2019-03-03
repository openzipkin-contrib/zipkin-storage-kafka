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
package zipkin2.storage.kafka;

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
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.GroupingSearch;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.SpanStore;
import zipkin2.storage.kafka.streams.stores.IndexStateStore;
import zipkin2.storage.kafka.streams.stores.IndexStoreType;

/**
 * Span Store based on Kafka Streams State Stores.
 */
public class KafkaSpanStore implements SpanStore {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaSpanStore.class);

  final String tracesStoreName;
  final String serviceStoreName;
  final String dependenciesStoreName;
  final String indexStoreName;

  final KafkaStreams aggregationStreams;
  final KafkaStreams indexStreams;

  KafkaSpanStore(KafkaStorage storage) {
    tracesStoreName = storage.tracesTopic.name;
    serviceStoreName = storage.servicesTopic.name;
    dependenciesStoreName = storage.dependenciesTopic.name;
    indexStoreName = storage.indexStoreName;
    aggregationStreams = storage.aggregationsStreams;
    indexStreams = storage.indexStreams;
  }

  @Override
  public Call<List<List<Span>>> getTraces(QueryRequest request) {
    ReadOnlyKeyValueStore<String, List<Span>> traceStore =
        aggregationStreams.store(tracesStoreName, QueryableStoreTypes.keyValueStore());
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
    protected List<List<Span>> doExecute() {
      return query();
    }

    private List<List<Span>> query()  {
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
      builder.add(LongPoint.newRangeQuery("ts", lowerValue, upperValue),
          BooleanClause.Occur.MUST);

      Sort sort = new Sort(new SortField("ts_sorted", SortField.Type.LONG, true));

      BooleanQuery query = builder.build();

      GroupingSearch groupingSearch = new GroupingSearch("trace_id_sorted");
      groupingSearch.setGroupSort(sort);
      TopGroups<BytesRef> docs =
          indexStateStore.groupSearch(groupingSearch, query, 0, queryRequest.limit());
      if (docs == null) return new ArrayList<>();

      Set<String> traceIds = new HashSet<>();

      for (GroupDocs<BytesRef> doc : docs.groups) {
        if (doc.groupValue != null) {
          String traceId = doc.groupValue.utf8ToString();
          traceIds.add(traceId);
        }
      }

      List<List<Span>> result = traceIds.stream()
          .map(traceStore::get)
          .collect(Collectors.toList());

      LOG.info("Total results of query {}: {}", query, result.size());

      return result;
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
    return new GetTraceCall(aggregationStreams, tracesStoreName, traceId);
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
    return new GetServiceNamesCall(aggregationStreams, serviceStoreName);
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
    return new GetSpanNamesCall(aggregationStreams, serviceStoreName, serviceName);
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
          if (spanNames == null) return new ArrayList<>();
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
    return new GetDependenciesCall(aggregationStreams, dependenciesStoreName);
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
