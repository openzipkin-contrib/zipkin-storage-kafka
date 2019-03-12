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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
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
  final String servicesStoreName;
  final String dependenciesStoreName;
  final String spanIndexStoreName;

  final KafkaStreams storeStreams;
  final KafkaStreams indexStreams;

  final KafkaStreams spanIndexStoreStream;
  final KafkaStreams traceStoreStream;
  final KafkaStreams serviceStoreStream;
  final KafkaStreams dependencyStoreStream;

  KafkaSpanStore(KafkaStorage storage) {
    tracesStoreName = storage.tracesTopic.name;
    servicesStoreName = storage.servicesTopic.name;
    dependenciesStoreName = storage.dependenciesTopic.name;
    spanIndexStoreName = storage.indexStoreName;
    storeStreams = storage.storeStreams;
    indexStreams = storage.indexStreams;
    // TODO: initialize
    spanIndexStoreStream = null;
    traceStoreStream = null;
    serviceStoreStream = null;
    dependencyStoreStream = null;
  }

  static Span hydrateSpan(Span lightSpan, Document document) {
    Span.Builder spanBuilder = Span.newBuilder()
        .id(lightSpan.id())
        .parentId(lightSpan.parentId())
        .traceId(lightSpan.traceId())
        .localEndpoint(lightSpan.localEndpoint())
        .remoteEndpoint(lightSpan.remoteEndpoint())
        .duration(lightSpan.duration())
        .timestamp(lightSpan.timestamp())
        .kind(lightSpan.kind());
    String[] annotations = document.getValues("annotation");
    String[] annotationTs = document.getValues("annotation_ts");
    String[] tags = document.getValues("tags");
    for (String tag : tags) {
      String[] tagParts = tag.split(":");
      spanBuilder.putTag(tagParts[0], tagParts[1]);
    }
    for (int i = 0; i < annotations.length; i++) {
      spanBuilder.addAnnotation(Long.valueOf(annotationTs[i]), annotations[i]);
    }
    return spanBuilder.build();
  }

  @Override
  public Call<List<List<Span>>> getTraces(QueryRequest request) {
    return new GetTracesCall(
        traceStoreStream,
        tracesStoreName,
        spanIndexStoreStream,
        spanIndexStoreName,
        request);
  }

  @Override
  public Call<List<Span>> getTrace(String traceId) {
    return new GetTraceCall(
        traceStoreStream,
        tracesStoreName,
        spanIndexStoreStream,
        spanIndexStoreName,
        traceId);
  }

  @Override
  public Call<List<String>> getServiceNames() {
    return new GetServiceNamesCall(serviceStoreStream, servicesStoreName);
  }

  @Override
  public Call<List<String>> getSpanNames(String serviceName) {
    return new GetSpanNamesCall(serviceStoreStream, servicesStoreName, serviceName);
  }

  @Override
  public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
    return new GetDependenciesCall(dependencyStoreStream, dependenciesStoreName);
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

  static class GetTracesCall extends Call.Base<List<List<Span>>> {
    final KafkaStreams traceStoreStream;
    final String tracesStoreName;
    final KafkaStreams spanIndexStoreStream;
    final String spanIndexStoreName;
    final QueryRequest queryRequest;

    GetTracesCall(
        KafkaStreams traceStoreStream,
        String tracesStoreName,
        KafkaStreams spanIndexStoreStream,
        String spanIndexStoreName,
        QueryRequest queryRequest) {
      this.traceStoreStream = traceStoreStream;
      this.tracesStoreName = tracesStoreName;
      this.spanIndexStoreStream = spanIndexStoreStream;
      this.spanIndexStoreName = spanIndexStoreName;
      this.queryRequest = queryRequest;
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

      IndexStateStore indexStateStore =
          spanIndexStoreStream.store(spanIndexStoreName, new IndexStoreType());

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

      ReadOnlyKeyValueStore<String, List<Span>> tracesStore =
          traceStoreStream.store(tracesStoreName, QueryableStoreTypes.keyValueStore());
      List<List<Span>> result = traceIds.stream()
          .map(tracesStore::get)
          .map(spans -> hydrateSpans(spans, indexStateStore))
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
      return new GetTracesCall(
          traceStoreStream,
          tracesStoreName,
          spanIndexStoreStream,
          spanIndexStoreName,
          queryRequest);
    }
  }

  static class GetTraceCall extends KafkaStreamsStoreCall<List<Span>> {
    final KafkaStreams traceStoreStream;
    final String tracesStoreName;
    final KafkaStreams spanIndexStoreStream;
    final String spanIndexStoreName;
    final String traceId;

    GetTraceCall(
        KafkaStreams traceStoreStream,
        String tracesStoreName,
        KafkaStreams spanIndexStoreStream,
        String spanIndexStoreName,
        String traceId) {
      this.traceStoreStream = traceStoreStream;
      this.spanIndexStoreStream = spanIndexStoreStream;
      this.tracesStoreName = tracesStoreName;
      this.spanIndexStoreName = spanIndexStoreName;
      this.traceId = traceId;
    }

    @Override
    Supplier<List<Span>> query() {
      return () -> {
        try {
          ReadOnlyKeyValueStore<String, List<Span>> traceStore =
              traceStoreStream.store(traceId, QueryableStoreTypes.keyValueStore());
          final List<Span> lightSpans = traceStore.get(traceId);
          IndexStateStore indexStateStore =
              spanIndexStoreStream.store(spanIndexStoreName, new IndexStoreType());
          return hydrateSpans(lightSpans, indexStateStore);
        } catch (Exception e) {
          LOG.error("Error getting trace with ID {}", traceId, e);
          return null;
        }
      };
    }

    @Override
    public Call<List<Span>> clone() {
      return new GetTraceCall(
          traceStoreStream,
          tracesStoreName,
          spanIndexStoreStream,
          spanIndexStoreName,
          traceId);
    }
  }

  private static List<Span> hydrateSpans(List<Span> lightSpans, IndexStateStore indexStateStore) {
    List<Span> hydratedSpans = new ArrayList<>();
    for (Span lightSpan : lightSpans) {
      Query query = new TermQuery(new Term("id", lightSpan.id()));
      Document document = indexStateStore.get(query);
      final Span span = hydrateSpan(lightSpan, document);
      hydratedSpans.add(span);
    }
    return hydratedSpans;
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
