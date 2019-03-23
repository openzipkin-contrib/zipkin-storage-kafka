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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.SpanStore;
import zipkin2.storage.kafka.index.SpanIndexService;

/**
 * Span Store based on Kafka Streams.
 *
 * This store supports all searches (e.g. findTraces, getTrace, getServiceNames, getSpanNames, and
 * getDependencies).
 *
 * NOTE: Currently State Stores are based on global state stores (i.e., all data is replicated on
 * every Zipkin instance with spanStoreEnabled=true).
 */
public class KafkaSpanStore implements SpanStore {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaSpanStore.class);
  // Store names
  final String tracesStoreName;
  final String servicesStoreName;
  final String dependenciesStoreName;
  // Kafka Streams
  final KafkaStreams traceStoreStream;
  final KafkaStreams serviceStoreStream;
  final KafkaStreams dependencyStoreStream;
  // Span index
  final SpanIndexService spanIndexService;

  KafkaSpanStore(KafkaStorage storage) {
    tracesStoreName = storage.traceStoreName;
    servicesStoreName = storage.serviceStoreName;
    dependenciesStoreName = storage.dependencyStoreName;
    traceStoreStream = storage.getTraceStoreStream();
    serviceStoreStream = storage.getServiceStoreStream();
    dependencyStoreStream = storage.getDependencyStoreStream();
    spanIndexService = storage.getSpanIndexService();
  }

  @Override
  public Call<List<List<Span>>> getTraces(QueryRequest request) {
    try {
      ReadOnlyKeyValueStore<String, List<Span>> traceStore =
          traceStoreStream.store(tracesStoreName, QueryableStoreTypes.keyValueStore());
      return new GetTracesCall(traceStore, spanIndexService, request);
    } catch (Exception e) {
      LOG.error("Error getting traces", request, e);
      return Call.emptyList();
    }
  }

  @Override
  public Call<List<Span>> getTrace(String traceId) {
    try {
      ReadOnlyKeyValueStore<String, List<Span>> traceStore =
          traceStoreStream.store(tracesStoreName, QueryableStoreTypes.keyValueStore());
      return new GetTraceCall(traceStore, traceId);
    } catch (Exception e) {
      LOG.error("Error getting trace {}", traceId, e);
      return Call.emptyList();
    }
  }

  @Override
  public Call<List<String>> getServiceNames() {
    try {
      ReadOnlyKeyValueStore<String, Set<String>> serviceStore =
          serviceStoreStream.store(servicesStoreName, QueryableStoreTypes.keyValueStore());
      return new GetServiceNamesCall(serviceStore);
    } catch (Exception e) {
      LOG.error("Error getting service names", e);
      return Call.emptyList();
    }
  }

  @Override
  public Call<List<String>> getSpanNames(String serviceName) {
    try {
      ReadOnlyKeyValueStore<String, Set<String>> serviceStore =
          serviceStoreStream.store(servicesStoreName, QueryableStoreTypes.keyValueStore());
      return new GetSpanNamesCall(serviceStore, serviceName);
    } catch (Exception e) {
      LOG.error("Error getting span names from service {}", serviceName, e);
      return Call.emptyList();
    }
  }

  @Override
  public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
    try {
      ReadOnlyKeyValueStore<Long, DependencyLink> dependenciesStore =
          dependencyStoreStream.
              store(dependenciesStoreName, QueryableStoreTypes.keyValueStore());
      return new GetDependenciesCall(endTs, lookback, dependenciesStore);
    } catch (Exception e) {
      LOG.error("Error getting dependencies", e);
      return Call.emptyList();
    }
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
    final SpanIndexService spanIndexService;
    final QueryRequest queryRequest;

    GetTracesCall(
        ReadOnlyKeyValueStore<String, List<Span>> traceStore,
        SpanIndexService spanIndexService,
        QueryRequest queryRequest) {
      this.traceStore = traceStore;
      this.spanIndexService = spanIndexService;
      this.queryRequest = queryRequest;
    }

    List<List<Span>> query() {
      List<List<Span>> result = new ArrayList<>();
      for (String traceId : spanIndexService.getTraceIds(queryRequest)) {
        List<Span> spans = traceStore.get(traceId);
        result.add(spans);
      }

      LOG.info("Total results of query {}: {}", queryRequest, result.size());

      return result;
    }

    @Override
    public Call<List<List<Span>>> clone() {
      return new GetTracesCall(traceStore, spanIndexService, queryRequest);
    }
  }

  static class GetTraceCall extends KafkaStreamsStoreCall<List<Span>> {
    final ReadOnlyKeyValueStore<String, List<Span>> traceStore;
    final String traceId;

    GetTraceCall(
        ReadOnlyKeyValueStore<String, List<Span>> traceStore,
        String traceId) {
      this.traceStore = traceStore;
      this.traceId = traceId;
    }

    @Override
    List<Span> query() {
        try {
          final List<Span> spans = traceStore.get(traceId);
          if (spans == null) return new ArrayList<>();
          return spans;
        } catch (Exception e) {
          LOG.error("Error getting trace with ID {}", traceId, e);
          return null;
        }
    }

    @Override
    public Call<List<Span>> clone() {
      return new GetTraceCall(traceStore, traceId);
    }
  }

  static class GetDependenciesCall extends KafkaStreamsStoreCall<List<DependencyLink>> {
    final long endTs, loopback;
    final ReadOnlyKeyValueStore<Long, DependencyLink> dependenciesStore;

    GetDependenciesCall(long endTs, long loopback,
        ReadOnlyKeyValueStore<Long, DependencyLink> dependenciesStore) {
      this.endTs = endTs;
      this.loopback = loopback;
      this.dependenciesStore = dependenciesStore;
    }

    @Override
    List<DependencyLink> query() {
        try {
          Map<String, DependencyLink> dependencyLinks = new HashMap<>();
          long from = endTs - loopback;
          dependenciesStore.range(from, endTs)
              .forEachRemaining(dependencyLink -> {
                String pair = String.format("%s-%s", dependencyLink.value.parent(),
                    dependencyLink.value.child());
                dependencyLinks.put(pair, dependencyLink.value);
              });

          LOG.info("Dependencies found from={}-to={}: {}", from, endTs, dependencyLinks.size());

          return new ArrayList<>(dependencyLinks.values());
        } catch (Exception e) {
          LOG.error("Error looking up for dependencies", e);
          return new ArrayList<>();
        }
    }

    @Override
    public Call<List<DependencyLink>> clone() {
      return new GetDependenciesCall(endTs, loopback, dependenciesStore);
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
