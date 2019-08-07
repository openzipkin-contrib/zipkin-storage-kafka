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

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.ServiceAndSpanNames;
import zipkin2.storage.SpanStore;

import java.io.IOException;
import java.util.*;

import static zipkin2.storage.kafka.streams.stores.DependencyStoreSupplier.DEPENDENCY_LINKS_BY_TIMESTAMP_STORE_NAME;
import static zipkin2.storage.kafka.streams.stores.TraceStoreSupplier.*;

/**
 * Span Store based on Kafka Streams.
 *
 * This store supports all searches (e.g. findTraces, getTrace, getServiceNames, getSpanNames, and
 * getDependencies).
 *
 * NOTE: Currently State Stores are based on global state stores (i.e., all data is replicated on
 * every Zipkin instance with spanStoreEnabled=true).
 */
public class KafkaSpanStore implements SpanStore, ServiceAndSpanNames {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaSpanStore.class);
  // Kafka Streams
  final KafkaStreams traceStoreStream;
  final KafkaStreams dependencyStoreStream;

  KafkaSpanStore(KafkaStorage storage) {
    traceStoreStream = storage.getTraceStoreStream();
    dependencyStoreStream = storage.getDependencyStoreStream();
  }

  @Override
  public Call<List<List<Span>>> getTraces(QueryRequest request) {
    try {
      ReadOnlyKeyValueStore<String, List<Span>> tracesStore =
              traceStoreStream.store(TRACES_STORE_NAME, QueryableStoreTypes.keyValueStore());
      ReadOnlyKeyValueStore<Long, Set<String>> traceIdsByTimestampStore =
              traceStoreStream.store(TRACES_STORE_NAME, QueryableStoreTypes.keyValueStore());
      return new GetTracesCall(tracesStore, traceIdsByTimestampStore, request);
    } catch (Exception e) {
      LOG.error("Error getting traces. Request: {}", request, e);
      return Call.emptyList();
    }
  }

  @Override
  public Call<List<Span>> getTrace(String traceId) {
    try {
      ReadOnlyKeyValueStore<String, List<Span>> traceStore =
              traceStoreStream.store(TRACES_STORE_NAME, QueryableStoreTypes.keyValueStore());
      return new GetTraceCall(traceStore, traceId);
    } catch (Exception e) {
      LOG.error("Error getting trace {}", traceId, e);
      return Call.emptyList();
    }
  }

  @Override
  public Call<List<String>> getServiceNames() {
    try {
      ReadOnlyKeyValueStore<String, String> serviceStore =
              traceStoreStream.store(SERVICE_NAMES_STORE_NAME, QueryableStoreTypes.keyValueStore());
      return new GetServiceNamesCall(serviceStore);
    } catch (Exception e) {
      LOG.error("Error getting service names", e);
      return Call.emptyList();
    }
  }

  @Override
  public Call<List<String>> getRemoteServiceNames(String s) {
    return null;
  }

  @Override
  public Call<List<String>> getSpanNames(String serviceName) {
    try {
      ReadOnlyKeyValueStore<String, Set<String>> spanNamesStore =
              traceStoreStream.store(SPAN_NAMES_STORE_NAME, QueryableStoreTypes.keyValueStore());
      return new GetSpanNamesCall(spanNamesStore, serviceName);
    } catch (Exception e) {
      LOG.error("Error getting span names from service {}", serviceName, e);
      return Call.emptyList();
    }
  }

  @Override
  public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
    try {
      ReadOnlyKeyValueStore<Long, List<DependencyLink>> dependenciesStore =
          dependencyStoreStream.
                  store(DEPENDENCY_LINKS_BY_TIMESTAMP_STORE_NAME, QueryableStoreTypes.keyValueStore());
      return new GetDependenciesCall(endTs, lookback, dependenciesStore);
    } catch (Exception e) {
      LOG.error("Error getting dependencies", e);
      return Call.emptyList();
    }
  }

  static class GetServiceNamesCall extends KafkaStreamsStoreCall<List<String>> {
    ReadOnlyKeyValueStore<String, String> serviceStore;

    GetServiceNamesCall(ReadOnlyKeyValueStore<String, String> serviceStore) {
      this.serviceStore = serviceStore;
    }

    @Override
    List<String> query() {
        try {
          List<String> serviceNames = new ArrayList<>();
          serviceStore.all().forEachRemaining(keyValue -> serviceNames.add(keyValue.value));
          Collections.sort(serviceNames);
          return serviceNames;
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
    final ReadOnlyKeyValueStore<String, Set<String>> spanNamesStore;
    final String serviceName;

    GetSpanNamesCall(ReadOnlyKeyValueStore<String, Set<String>> spanNamesStore, String serviceName) {
      this.spanNamesStore = spanNamesStore;
      this.serviceName = serviceName;
    }

    @Override
    List<String> query() {
        try {
          if (serviceName == null || serviceName.equals("all")) return new ArrayList<>();
          Set<String> spanNamesSet = spanNamesStore.get(serviceName);
          if (spanNamesSet == null) return new ArrayList<>();
          List<String> spanNames = new ArrayList<>(spanNamesSet);
          Collections.sort(spanNames);
          return spanNames;
        } catch (Exception e) {
          LOG.error("Error looking up for span names for service {}", serviceName, e);
          return new ArrayList<>();
        }
    }

    @Override
    public Call<List<String>> clone() {
      return new GetSpanNamesCall(spanNamesStore, serviceName);
    }
  }

  static class GetTracesCall extends KafkaStreamsStoreCall<List<List<Span>>> {
    final ReadOnlyKeyValueStore<String, List<Span>> tracesStore;
    final ReadOnlyKeyValueStore<Long, Set<String>> traceIdsByTimestampStore;
    final QueryRequest queryRequest;

    GetTracesCall(
            ReadOnlyKeyValueStore<String, List<Span>> tracesStore,
            ReadOnlyKeyValueStore<Long, Set<String>> traceIdsByTimestampStore,
            QueryRequest queryRequest) {
      this.tracesStore = tracesStore;
      this.traceIdsByTimestampStore = traceIdsByTimestampStore;
      this.queryRequest = queryRequest;
    }

    @Override
    List<List<Span>> query() {
      List<List<Span>> result = new ArrayList<>();
      List<String> traceIds = new ArrayList<>();
      KeyValueIterator<Long, Set<String>> spanIds = traceIdsByTimestampStore.range(queryRequest.lookback(), queryRequest.endTs());
      spanIds.forEachRemaining(keyValue -> {
        for (String traceId : keyValue.value) {
          if (!traceIds.contains(traceId) && result.size() <= queryRequest.limit()) {
            List<Span> spans = tracesStore.get(traceId);
            if (queryRequest.test(spans)) {
              traceIds.add(traceId);
              result.add(spans);
            }
          }
        }
      });

      LOG.info("Total results of query {}: {}", queryRequest, result.size());

      return result;
    }

    @Override
    public Call<List<List<Span>>> clone() {
      return new GetTracesCall(tracesStore, traceIdsByTimestampStore, queryRequest);
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
    final ReadOnlyKeyValueStore<Long, List<DependencyLink>> dependenciesStore;

    GetDependenciesCall(long endTs, long loopback,
        ReadOnlyKeyValueStore<Long, List<DependencyLink>> dependenciesStore) {
      this.endTs = endTs;
      this.loopback = loopback;
      this.dependenciesStore = dependenciesStore;
    }

    @Override
    List<DependencyLink> query() {
        try {
          long from = endTs - loopback;
          Map<String, Long> dependencyLinksTime = new HashMap<>();
          Map<String, DependencyLink> dependencyLinks = new HashMap<>();
          dependenciesStore.range(from, endTs)
              .forEachRemaining(keyValue -> {
                Long ts = keyValue.key;
                for (DependencyLink link : keyValue.value) {
                  String pair = String.format("%s|%s", link.parent(), link.child());
                  Long last = dependencyLinksTime.get(pair);
                  if (last < ts) {
                    dependencyLinks.put(pair, link);
                    dependencyLinksTime.put(pair, ts);
                  }
                }
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
