/*
 * Copyright 2019-2020 The OpenZipkin Authors
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.state.HostInfo;
import zipkin2.Call;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.codec.DependencyLinkBytesDecoder;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.ServiceAndSpanNames;
import zipkin2.storage.SpanStore;
import zipkin2.storage.Traces;
import zipkin2.storage.kafka.internal.KafkaStoreListCall;
import zipkin2.storage.kafka.internal.KafkaStoreScatterGatherListCall;
import zipkin2.storage.kafka.internal.KafkaStoreSingleKeyListCall;
import zipkin2.storage.kafka.streams.DependencyStorageTopology;
import zipkin2.storage.kafka.streams.TraceStorageTopology;

import static zipkin2.storage.kafka.streams.DependencyStorageTopology.DEPENDENCIES_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStorageTopology.REMOTE_SERVICE_NAMES_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStorageTopology.SPAN_NAMES_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStorageTopology.TRACES_STORE_NAME;

/**
 * Span store backed by Kafka Stream distributed state stores built by {@link
 * TraceStorageTopology} and {@link DependencyStorageTopology}, and made accessible by
 * {@link  KafkaStorageHttpService}.
 */
final class KafkaSpanStore implements SpanStore, Traces, ServiceAndSpanNames {
  static final ObjectMapper MAPPER = new ObjectMapper();
  final KafkaStorage storage;
  final BiFunction<String, Integer, String> httpBaseUrl;
  final boolean traceSearchEnabled, traceByIdQueryEnabled, dependencyQueryEnabled;

  KafkaSpanStore(KafkaStorage storage) {
    this.storage = storage;
    httpBaseUrl = storage.httpBaseUrl;
    traceByIdQueryEnabled = storage.traceByIdQueryEnabled;
    traceSearchEnabled = storage.traceSearchEnabled;
    dependencyQueryEnabled = storage.dependencyQueryEnabled;
  }

  @Override public Call<List<List<Span>>> getTraces(QueryRequest request) {
    if (traceSearchEnabled) {
      return new GetTracesCall(storage.getTraceStorageStream(), httpBaseUrl, request);
    } else {
      return Call.emptyList();
    }
  }

  @Override public Call<List<Span>> getTrace(String traceId) {
    if (traceByIdQueryEnabled) {
      return new GetTraceCall(storage.getTraceStorageStream(), httpBaseUrl,
          Span.normalizeTraceId(traceId));
    } else {
      return Call.emptyList();
    }
  }

  @Override public Call<List<List<Span>>> getTraces(Iterable<String> traceIds) {
    if (traceByIdQueryEnabled) {
      StringJoiner joiner = new StringJoiner(",");
      for (String traceId : traceIds) {
        joiner.add(Span.normalizeTraceId(traceId));
      }

      if (joiner.length() == 0) return Call.emptyList();
      return new GetTraceManyCall(storage.getTraceStorageStream(), httpBaseUrl, joiner.toString());
    } else {
      return Call.emptyList();
    }
  }

  @Deprecated @Override public Call<List<String>> getServiceNames() {
    if (traceSearchEnabled) {
      return new GetServiceNamesCall(storage.getTraceStorageStream(), httpBaseUrl);
    } else {
      return Call.emptyList();
    }
  }

  @Deprecated @Override public Call<List<String>> getSpanNames(String serviceName) {
    if (traceSearchEnabled) {
      return new GetSpanNamesCall(storage.getTraceStorageStream(), serviceName, httpBaseUrl);
    } else {
      return Call.emptyList();
    }
  }

  @Override public Call<List<String>> getRemoteServiceNames(String serviceName) {
    if (traceSearchEnabled) {
      return new GetRemoteServiceNamesCall(storage.getTraceStorageStream(), serviceName, httpBaseUrl);
    } else {
      return Call.emptyList();
    }
  }

  @Override public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
    if (dependencyQueryEnabled) {
      return new GetDependenciesCall(storage.getDependencyStorageStream(), httpBaseUrl, endTs, lookback);
    } else {
      return Call.emptyList();
    }
  }

  static final class GetServiceNamesCall extends KafkaStoreScatterGatherListCall<String> {
    static final long SERVICE_NAMES_LIMIT = 1_000;
    final KafkaStreams traceStoreStream;
    final BiFunction<String, Integer, String> httpBaseUrl;

    GetServiceNamesCall(KafkaStreams traceStoreStream,
        BiFunction<String, Integer, String> httpBaseUrl) {
      super(
          traceStoreStream,
          SPAN_NAMES_STORE_NAME,
          httpBaseUrl,
          "/serviceNames",
          SERVICE_NAMES_LIMIT);
      this.traceStoreStream = traceStoreStream;
      this.httpBaseUrl = httpBaseUrl;
    }

    @Override protected String parseItem(JsonNode node) {
      return node.textValue();
    }

    @Override public Call<List<String>> clone() {
      return new GetServiceNamesCall(traceStoreStream, httpBaseUrl);
    }
  }

  static final class GetSpanNamesCall extends KafkaStoreSingleKeyListCall<String> {
    final KafkaStreams traceStoreStream;
    final String serviceName;
    final BiFunction<String, Integer, String> httpBaseUrl;

    GetSpanNamesCall(KafkaStreams traceStoreStream, String serviceName,
        BiFunction<String, Integer, String> httpBaseUrl) {
      super(traceStoreStream, SPAN_NAMES_STORE_NAME, httpBaseUrl,
          "/serviceNames/" + serviceName + "/spanNames", serviceName);
      this.traceStoreStream = traceStoreStream;
      this.serviceName = serviceName;
      this.httpBaseUrl = httpBaseUrl;
    }

    @Override protected String parseItem(JsonNode node) {
      return node.textValue();
    }

    @Override public Call<List<String>> clone() {
      return new GetSpanNamesCall(traceStoreStream, serviceName, httpBaseUrl);
    }
  }

  static final class GetRemoteServiceNamesCall extends KafkaStoreSingleKeyListCall<String> {
    final KafkaStreams traceStoreStream;
    final String serviceName;
    final BiFunction<String, Integer, String> httpBaseUrl;

    GetRemoteServiceNamesCall(KafkaStreams traceStoreStream, String serviceName,
        BiFunction<String, Integer, String> httpBaseUrl) {
      super(traceStoreStream, REMOTE_SERVICE_NAMES_STORE_NAME, httpBaseUrl,
          "/serviceNames/" + serviceName + "/remoteServiceNames", serviceName);
      this.traceStoreStream = traceStoreStream;
      this.serviceName = serviceName;
      this.httpBaseUrl = httpBaseUrl;
    }

    @Override protected String parseItem(JsonNode node) {
      return node.textValue();
    }

    @Override public Call<List<String>> clone() {
      return new GetRemoteServiceNamesCall(traceStoreStream, serviceName, httpBaseUrl);
    }
  }

  static final class GetTracesCall extends KafkaStoreScatterGatherListCall<List<Span>> {
    final KafkaStreams traceStoreStream;
    final BiFunction<String, Integer, String> httpBaseUrl;
    final QueryRequest request;

    GetTracesCall(KafkaStreams traceStoreStream,
        BiFunction<String, Integer, String> httpBaseUrl,
        QueryRequest request) {
      super(
          traceStoreStream,
          TRACES_STORE_NAME,
          httpBaseUrl,
          ("/traces?"
              + (request.serviceName() == null ? "" : "serviceName=" + request.serviceName() + "&")
              + (request.remoteServiceName() == null ? ""
              : "remoteServiceName=" + request.remoteServiceName() + "&")
              + (request.spanName() == null ? "" : "spanName=" + request.spanName() + "&")
              + (request.annotationQueryString() == null ? ""
              : "annotationQuery=" + request.annotationQueryString() + "&")
              + (request.minDuration() == null ? "" : "minDuration=" + request.minDuration() + "&")
              + (request.maxDuration() == null ? "" : "maxDuration=" + request.maxDuration() + "&")
              + ("endTs=" + request.endTs() + "&")
              + ("lookback=" + request.lookback() + "&")
              + ("limit=" + request.limit())),
          request.limit());
      this.traceStoreStream = traceStoreStream;
      this.httpBaseUrl = httpBaseUrl;
      this.request = request;
    }

    @Override protected List<Span> parseItem(JsonNode node) throws JsonProcessingException {
      return SpanBytesDecoder.JSON_V2.decodeList(MAPPER.writeValueAsBytes(node));
    }

    @Override public Call<List<List<Span>>> clone() {
      return new GetTracesCall(traceStoreStream, httpBaseUrl, request);
    }
  }

  static final class GetTraceCall extends KafkaStoreSingleKeyListCall<Span> {
    final KafkaStreams traceStoreStream;
    final BiFunction<String, Integer, String> httpBaseUrl;
    final String traceId;

    GetTraceCall(KafkaStreams traceStoreStream,
        BiFunction<String, Integer, String> httpBaseUrl,
        String traceId) {
      super(traceStoreStream, TRACES_STORE_NAME, httpBaseUrl, String.format("/traces/%s", traceId),
          traceId);
      this.traceStoreStream = traceStoreStream;
      this.httpBaseUrl = httpBaseUrl;
      this.traceId = traceId;
    }

    @Override protected Span parseItem(JsonNode node) throws JsonProcessingException {
      return SpanBytesDecoder.JSON_V2.decodeOne(MAPPER.writeValueAsBytes(node));
    }

    @Override public Call<List<Span>> clone() {
      return new GetTraceCall(traceStoreStream, httpBaseUrl, traceId);
    }
  }

  static final class GetTraceManyCall extends KafkaStoreListCall<List<Span>> {
    static final StringSerializer STRING_SERIALIZER = new StringSerializer();

    final KafkaStreams traceStoreStream;
    final BiFunction<String, Integer, String> httpBaseUrl;
    final String traceIds;

    GetTraceManyCall(KafkaStreams traceStoreStream,
        BiFunction<String, Integer, String> httpBaseUrl,
        String traceIds) {
      super(traceStoreStream, TRACES_STORE_NAME, httpBaseUrl, "/traceMany?traceIds=" + traceIds);
      this.traceStoreStream = traceStoreStream;
      this.httpBaseUrl = httpBaseUrl;
      this.traceIds = traceIds;
    }

    @Override protected List<Span> parseItem(JsonNode node) throws JsonProcessingException {
      return SpanBytesDecoder.JSON_V2.decodeList(MAPPER.writeValueAsBytes(node));
    }

    @Override public Call<List<List<Span>>> clone() {
      return new GetTraceManyCall(traceStoreStream, httpBaseUrl, traceIds);
    }

    @Override
    protected CompletableFuture<List<List<Span>>> listFuture() {
      // To reduce calls to store instances traceIds are grouped by hostInfo
      Map<HostInfo, List<String>> traceIdsByHost = new LinkedHashMap<>();
      for (String traceId : traceIds.split(",", 1_000)) {
        KeyQueryMetadata metadata =
            traceStoreStream.queryMetadataForKey(TRACES_STORE_NAME, traceId, STRING_SERIALIZER);
        List<String> collected = traceIdsByHost.get(metadata.activeHost());
        if (collected == null) collected = new ArrayList<>();
        collected.add(traceId);
        traceIdsByHost.put(metadata.activeHost(), collected);
      }
      // Only calls to hosts that have traceIds are executed
      List<CompletableFuture<AggregatedHttpResponse>> responseFutures =
          traceIdsByHost.entrySet()
              .stream()
              .map(entry -> httpClient(entry.getKey())
                  .get("/traceMany?traceIds=" + String.join(",", entry.getValue())))
              .map(HttpResponse::aggregate)
              .collect(Collectors.toList());
      return CompletableFuture.allOf(responseFutures.toArray(new CompletableFuture[0]))
          .thenApply(unused ->
              responseFutures.stream()
                  .map(s -> s.getNow(AggregatedHttpResponse.of(HttpStatus.INTERNAL_SERVER_ERROR)))
                  .map(this::content)
                  .map(this::parseList)
                  .flatMap(Collection::stream)
                  .distinct()
                  .collect(Collectors.toList()));
    }
  }

  static final class GetDependenciesCall extends KafkaStoreScatterGatherListCall<DependencyLink> {
    static final long DEPENDENCIES_LIMIT = 1_000;

    final KafkaStreams dependencyStoreStream;
    final BiFunction<String, Integer, String> httpBaseUrl;
    final long endTs, lookback;

    GetDependenciesCall(KafkaStreams dependencyStoreStream,
        BiFunction<String, Integer, String> httpBaseUrl,
        long endTs, long lookback) {
      super(
          dependencyStoreStream,
          DEPENDENCIES_STORE_NAME,
          httpBaseUrl,
          "/dependencies?endTs=" + endTs + "&lookback=" + lookback,
          DEPENDENCIES_LIMIT);
      this.dependencyStoreStream = dependencyStoreStream;
      this.httpBaseUrl = httpBaseUrl;
      this.endTs = endTs;
      this.lookback = lookback;
    }

    @Override protected DependencyLink parseItem(JsonNode node) throws JsonProcessingException {
      return DependencyLinkBytesDecoder.JSON_V1.decodeOne(MAPPER.writeValueAsBytes(node));
    }

    @Override public Call<List<DependencyLink>> clone() {
      return new GetDependenciesCall(dependencyStoreStream, httpBaseUrl, endTs, lookback);
    }
  }
}
