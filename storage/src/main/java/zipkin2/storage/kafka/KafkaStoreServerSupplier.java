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

import com.google.gson.Gson;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.Service;
import com.linecorp.armeria.server.annotation.ConsumesJson;
import com.linecorp.armeria.server.annotation.Get;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;

import static zipkin2.storage.kafka.streams.TraceStoreTopologySupplier.REMOTE_SERVICE_NAMES_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStoreTopologySupplier.SERVICE_NAMES_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStoreTopologySupplier.SPAN_IDS_BY_TS_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStoreTopologySupplier.SPAN_NAMES_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStoreTopologySupplier.TRACES_STORE_NAME;

public class KafkaStoreServerSupplier implements Supplier<Server> {
  static final Gson GSON = new Gson();

  final KafkaStreams traceStoreStream;
  final KafkaStreams dependencyStoreStream;
  final long minTracesStored;

  public KafkaStoreServerSupplier(KafkaStorage storage) {
    this.traceStoreStream = storage.getTraceStoreStream();
    this.dependencyStoreStream = storage.getDependencyStoreStream();
    this.minTracesStored = storage.minTracesStored;
  }

  @Override public Server get() {
    ServerBuilder builder = new ServerBuilder();
    builder.http(9412);
    // Metadata-related services
    builder.service("/instances", getInstances());
    builder.service("/instances/:store_name", getStoreHostNameInfo());
    // Traces-related services
    builder.annotatedService(getTracesKeyValues()); // kv.range() + foreach(kv.get()).filter()
    builder.service("/traces/:trace_id", getTraceKeyValue()); // kv.get()
    // Service names related
    builder.service("/service_names", getServiceNames()); // kv.all()
    builder.service("/service_names/:service_name/span_names",
        getSpanNamesByServiceName()); // kv.get()
    builder.service("/service_names/:service_name/remote_service_names",
        getRemoteServiceNamesByServiceName()); // kv.get()
    return builder.build();
  }

  private Service<HttpRequest, HttpResponse> getRemoteServiceNamesByServiceName() {
    return (ctx, req) -> {
      String serviceName = ctx.pathParam("service_name");
      ReadOnlyKeyValueStore<String, Set<String>> store =
          traceStoreStream.store(REMOTE_SERVICE_NAMES_STORE_NAME,
              QueryableStoreTypes.keyValueStore());
      Set<String> names = store.get(serviceName);
      return HttpResponse.of(MediaType.JSON, GSON.toJson(names));
    };
  }

  Service<HttpRequest, HttpResponse> getSpanNamesByServiceName() {
    return (ctx, req) -> {
      String serviceName = ctx.pathParam("service_name");
      ReadOnlyKeyValueStore<String, Set<String>> store =
          traceStoreStream.store(SPAN_NAMES_STORE_NAME,
              QueryableStoreTypes.keyValueStore());
      Set<String> names = store.get(serviceName);
      return HttpResponse.of(MediaType.JSON, GSON.toJson(names));
    };
  }

  Service<HttpRequest, HttpResponse> getServiceNames() {
    return (ctx, req) -> {
      ReadOnlyKeyValueStore<String, String> store =
          traceStoreStream.store(SERVICE_NAMES_STORE_NAME,
              QueryableStoreTypes.keyValueStore());
      Set<String> names = new HashSet<>();
      store.all().forEachRemaining(keyValue -> names.add(keyValue.value));
      return HttpResponse.of(MediaType.JSON, GSON.toJson(names));
    };
  }

  Object getTracesKeyValues() {
    return new Object() {
      ReadOnlyKeyValueStore<String, List<Span>> tracesStore =
          traceStoreStream.store(TRACES_STORE_NAME, QueryableStoreTypes.keyValueStore());
      ReadOnlyKeyValueStore<Long, Set<String>> traceIdsByTsStore =
          traceStoreStream.store(SPAN_IDS_BY_TS_STORE_NAME, QueryableStoreTypes.keyValueStore());

      @Get("/traces")
      @ConsumesJson
      public HttpResponse getTraces(String requestJson) {
        QueryRequest request = GSON.fromJson(requestJson, QueryRequest.Builder.class).build();
        List<List<Span>> traces = new ArrayList<>();
        List<String> traceIds = new ArrayList<>();
        // milliseconds to microseconds
        long from = (request.endTs() - request.lookback()) * 1000;
        long to = request.endTs() * 1000;
        int bucket = 30 * 1000 * 1000;
        long checkpoint = to - bucket; // 30 sec before upper bound
        if (checkpoint <= from
            || tracesStore.approximateNumEntries() <= minTracesStored) { // do one run
          try (KeyValueIterator<Long, Set<String>> spanIds = traceIdsByTsStore.range(from, to)) {
            spanIds.forEachRemaining(keyValue -> {
              for (String traceId : keyValue.value) {
                if (!traceIds.contains(traceId)) {
                  List<Span> spans = tracesStore.get(traceId);
                  if (spans != null && request.test(spans)) { // apply filters
                    traceIds.add(traceId); // adding to check if we have already add it later
                    traces.add(spans);
                  }
                }
              }
            });
          }
        } else {
          while (checkpoint > from && traces.size() < request.limit()) {
            try (KeyValueIterator<Long, Set<String>> spanIds = traceIdsByTsStore.range(checkpoint,
                to)) {
              spanIds.forEachRemaining(keyValue -> {
                for (String traceId : keyValue.value) {
                  if (!traceIds.contains(traceId)) {
                    List<Span> spans = tracesStore.get(traceId);
                    if (spans != null && request.test(spans)) { // apply filters
                      traceIds.add(traceId); // adding to check if we have already add it later
                      traces.add(spans);
                    }
                  }
                }
              });
            }
            to = checkpoint;
            checkpoint = checkpoint - bucket; // 1 min before more
          }
        }
        traces.sort(
            Comparator.<List<Span>>comparingLong(o -> o.get(0).timestampAsLong()).reversed());
        //LOG.debug("Traces found from query {}: {}", request, traces.size());
        return HttpResponse.of(MediaType.JSON, GSON.toJson(traces.subList(0,
            request.limit() >= traces.size() ? traceIds.size() : request.limit())));
      }
    };
  }

  Service<HttpRequest, HttpResponse> getTraceKeyValue() {
    return (ctx, req) -> {
      String traceId = ctx.pathParam("trace_id");
      ReadOnlyKeyValueStore<String, List<Span>> store =
          traceStoreStream.store(TRACES_STORE_NAME, QueryableStoreTypes.keyValueStore());
      List<Span> spans = store.get(traceId);
      return HttpResponse.of(MediaType.JSON, GSON.toJson(spans));
    };
  }

  Service<HttpRequest, HttpResponse> getStoreHostNameInfo() {
    return (ctx, req) -> {
      String storeName = ctx.pathParam("store_name");
      return HttpResponse.of(MediaType.JSON,
          GSON.toJson(traceStoreStream.allMetadataForStore(storeName)));
    };
  }

  Service<HttpRequest, HttpResponse> getInstances() {
    return (ctx, req) -> HttpResponse.of(
        MediaType.JSON, GSON.toJson(traceStoreStream.allMetadata()));
  }
}
