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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.grouping.GroupingSearch;
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
 *
 * This store supports all searches (e.g. findTraces, getTrace, getServiceNames, getSpanNames, and
 * getDependencies).
 *
 * NOTE: Currently State Stores are based on global state stores (i.e., all data is replicated on
 * every Zipkin instance with spanStoreEnabled=true.
 */
public class KafkaSpanStore implements SpanStore {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaSpanStore.class);

  final String tracesStoreName;
  final String servicesStoreName;
  final String dependenciesStoreName;
  final String spanIndexStoreName;

  final KafkaStreams spanIndexStream;
  final KafkaStreams traceStoreStream;
  final KafkaStreams traceAggregationStream;
  final KafkaStreams serviceStoreStream;
  final KafkaStreams dependencyStoreStream;
  final KafkaStreams traceRetentionStoreStream;

  KafkaSpanStore(KafkaStorage storage) {
    tracesStoreName = storage.traceStoreName;
    servicesStoreName = storage.serviceStoreName;
    dependenciesStoreName = storage.dependencyStoreName;
    spanIndexStoreName = storage.spanIndexStoreName;

    spanIndexStream = storage.getSpanIndexStream();
    traceStoreStream = storage.getTraceStoreStream();
    serviceStoreStream = storage.getServiceStoreStream();
    dependencyStoreStream = storage.getDependencyStoreStream();
    traceAggregationStream = storage.getTraceAggregationStream();
    traceRetentionStoreStream = storage.getTraceRetentionStream();
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

    String[] tags = document.getValues("tag");
    for (String tag : tags) {
      String[] tagParts = tag.split("=");
      spanBuilder.putTag(tagParts[0], tagParts[1]);
    }

    String[] annotations = document.getValues("annotation_value");
    String[] annotationTs = document.getValues("annotation_ts");
    for (int i = 0; i < annotations.length; i++) {
      spanBuilder.addAnnotation(Long.valueOf(annotationTs[i]), annotations[i]);
    }

    return spanBuilder.build();
  }

  @Override
  public Call<List<List<Span>>> getTraces(QueryRequest request) {
    ReadOnlyKeyValueStore<String, List<Span>> traceStore =
        traceStoreStream.store(tracesStoreName, QueryableStoreTypes.keyValueStore());
    IndexStateStore spanIndexStore =
        spanIndexStream.store(spanIndexStoreName, new IndexStoreType());
    return new GetTracesCall(traceStore, spanIndexStore, request);
  }

  @Override
  public Call<List<Span>> getTrace(String traceId) {
    ReadOnlyKeyValueStore<String, List<Span>> traceStore =
        traceStoreStream.store(tracesStoreName, QueryableStoreTypes.keyValueStore());
    IndexStateStore spanIndexStore =
        spanIndexStream.store(spanIndexStoreName, new IndexStoreType());
    return new GetTraceCall(traceStore, spanIndexStore, traceId);
  }

  @Override
  public Call<List<String>> getServiceNames() {
    ReadOnlyKeyValueStore<String, Set<String>> serviceStore =
        serviceStoreStream.store(servicesStoreName, QueryableStoreTypes.keyValueStore());
    return new GetServiceNamesCall(serviceStore);
  }

  @Override
  public Call<List<String>> getSpanNames(String serviceName) {
    ReadOnlyKeyValueStore<String, Set<String>> serviceStore =
        serviceStoreStream.store(servicesStoreName, QueryableStoreTypes.keyValueStore());
    return new GetSpanNamesCall(serviceStore, serviceName);
  }

  @Override
  public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
    ReadOnlyKeyValueStore<String, DependencyLink> dependenciesStore =
        dependencyStoreStream.store(dependenciesStoreName, QueryableStoreTypes.keyValueStore());
    return new GetDependenciesCall(dependenciesStore);
  }

  static class GetServiceNamesCall extends KafkaStreamsStoreCall<List<String>> {
    ReadOnlyKeyValueStore<String, Set<String>> serviceStore;

    GetServiceNamesCall(ReadOnlyKeyValueStore<String, Set<String>> serviceStore) {
      this.serviceStore = serviceStore;
    }

    @Override
    List<String> query() {
        try {
          List<String> keys = new ArrayList<>();
          serviceStore.all().forEachRemaining(keyValue -> keys.add(keyValue.key));
          return keys;
        } catch (Exception e) {
          LOG.error("Error looking up services", e);
          return new ArrayList<>();
        }
    }

    @Override
    public Call<List<String>> clone() {
      return new GetServiceNamesCall(serviceStore);
    }
  }

  static class GetSpanNamesCall extends KafkaStreamsStoreCall<List<String>> {
    final ReadOnlyKeyValueStore<String, Set<String>> serviceStore;
    final String serviceName;

    GetSpanNamesCall(ReadOnlyKeyValueStore<String, Set<String>> serviceStore, String serviceName) {
      this.serviceStore = serviceStore;
      this.serviceName = serviceName;
    }

    @Override
    List<String> query() {
        try {
          if (serviceName == null || serviceName.equals("all")) return new ArrayList<>();
          Set<String> spanNames = serviceStore.get(serviceName);
          if (spanNames == null) return new ArrayList<>();
          return new ArrayList<>(spanNames);
        } catch (Exception e) {
          LOG.error("Error looking up for span names for service {}", serviceName, e);
          return new ArrayList<>();
        }
    }

    @Override
    public Call<List<String>> clone() {
      return new GetSpanNamesCall(serviceStore, serviceName);
    }
  }

  static class GetTracesCall extends KafkaStreamsStoreCall<List<List<Span>>> {
    final ReadOnlyKeyValueStore<String, List<Span>> traceStore;
    final IndexStateStore spanIndexStore;
    final QueryRequest queryRequest;

    GetTracesCall(
        ReadOnlyKeyValueStore<String, List<Span>> traceStore,
        IndexStateStore spanIndexStore,
        QueryRequest queryRequest) {
      this.traceStore = traceStore;
      this.spanIndexStore = spanIndexStore;
      this.queryRequest = queryRequest;
    }

    List<List<Span>> query() {
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

      if (queryRequest.annotationQueryString() != null) {
        try {
          QueryParser queryParser = new QueryParser("annotation", new StandardAnalyzer());
          Query annotationQuery = queryParser.parse(queryRequest.annotationQueryString());
          builder.add(annotationQuery, BooleanClause.Occur.MUST);
        } catch (ParseException e) {
          e.printStackTrace();
        }
      }

      if (queryRequest.maxDuration() != null) {
        Query durationRangeQuery = LongPoint.newRangeQuery(
            "duration", queryRequest.minDuration(), queryRequest.maxDuration());
        builder.add(durationRangeQuery, BooleanClause.Occur.MUST);
      }

      long start = queryRequest.endTs() - queryRequest.lookback();
      long end = queryRequest.endTs();
      //TODO No timestamp field in Lucene. Find a way to query timestamp instead of longs.
      long lowerValue = Long.parseLong(start + "000");
      long upperValue = Long.parseLong(end + "000");
      Query tsRangeQuery = LongPoint.newRangeQuery("ts", lowerValue, upperValue);
      builder.add(tsRangeQuery, BooleanClause.Occur.MUST);

      BooleanQuery query = builder.build();

      GroupingSearch groupingSearch = new GroupingSearch("trace_id_sorted");

      Sort sort = new Sort(new SortField("ts_sorted", SortField.Type.LONG, true));
      groupingSearch.setGroupDocsLimit(1);
      groupingSearch.setGroupSort(sort);

      List<Document> docs =
          spanIndexStore.groupSearch(groupingSearch, query, 0, queryRequest.limit());

      List<List<Span>> result = new ArrayList<>();

      for (Document doc : docs) {
        String traceId = doc.get("trace_id");
        List<Span> spans = traceStore.get(traceId);
        result.add(hydrateSpans(spans, spanIndexStore));
      }

      LOG.info("Total results of query {}: {}", query, result.size());

      return result;
    }

    @Override
    public Call<List<List<Span>>> clone() {
      return new GetTracesCall(
          traceStore,
          spanIndexStore,
          queryRequest);
    }
  }

  static class GetTraceCall extends KafkaStreamsStoreCall<List<Span>> {
    final ReadOnlyKeyValueStore<String, List<Span>> traceStore;
    final IndexStateStore spanIndexStore;
    final String traceId;

    GetTraceCall(
        ReadOnlyKeyValueStore<String, List<Span>> traceStore,
        IndexStateStore spanIndexStore, String traceId) {
      this.traceStore = traceStore;
      this.spanIndexStore = spanIndexStore;
      this.traceId = traceId;
    }

    @Override
    List<Span> query() {
        try {
          final List<Span> lightSpans = traceStore.get(traceId);
          if (lightSpans == null) return new ArrayList<>();
          return hydrateSpans(lightSpans, spanIndexStore);
        } catch (Exception e) {
          LOG.error("Error getting trace with ID {}", traceId, e);
          return null;
        }
    }

    @Override
    public Call<List<Span>> clone() {
      return new GetTraceCall(traceStore, spanIndexStore, traceId);
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

  static class GetDependenciesCall extends KafkaStreamsStoreCall<List<DependencyLink>> {
    final ReadOnlyKeyValueStore<String, DependencyLink> dependenciesStore;

    GetDependenciesCall(ReadOnlyKeyValueStore<String, DependencyLink> dependenciesStore) {
      this.dependenciesStore = dependenciesStore;
    }

    @Override
    List<DependencyLink> query() {
        try {
          List<DependencyLink> dependencyLinks = new ArrayList<>();
          dependenciesStore.all()
              .forEachRemaining(dependencyLink -> dependencyLinks.add(dependencyLink.value));
          return dependencyLinks;
        } catch (Exception e) {
          LOG.error("Error looking up for dependencies", e);
          return new ArrayList<>();
        }
    }

    @Override
    public Call<List<DependencyLink>> clone() {
      return new GetDependenciesCall(dependenciesStore);
    }
  }

  abstract static class KafkaStreamsStoreCall<T> extends Call.Base<T> {

    KafkaStreamsStoreCall() {
    }

    @Override
    protected T doExecute() throws IOException {
      try {
        return query();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    protected void doEnqueue(Callback<T> callback) {
      try {
        callback.onSuccess(query());
      } catch (Exception e) {
        callback.onError(e);
      }
    }

    abstract T query();
  }
}
