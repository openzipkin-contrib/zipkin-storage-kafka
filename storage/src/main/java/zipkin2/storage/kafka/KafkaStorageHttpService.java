/*
 * Copyright 2019-2024 The OpenZipkin Authors
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.annotation.Default;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.ProducesJson;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.codec.DependencyLinkBytesEncoder;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.internal.DependencyLinker;
import zipkin2.storage.QueryRequest;

import static zipkin2.storage.kafka.streams.DependencyStorageTopology.DEPENDENCIES_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStorageTopology.AUTOCOMPLETE_TAGS_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStorageTopology.REMOTE_SERVICE_NAMES_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStorageTopology.SPAN_NAMES_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStorageTopology.TRACES_STORE_NAME;

/**
 * Server to enable access to local stores.
 *
 * <p>Given the partitioned nature of local stores, a RPC layer is required to allow accessing
 * distributed state. This component exposes access to local state via Http call from {@link
 * KafkaSpanStore}
 */
final class KafkaStorageHttpService {
  static final Logger LOG = LoggerFactory.getLogger(KafkaStorageHttpService.class);
  static final ObjectMapper MAPPER = new ObjectMapper();

  final KafkaStorage storage;
  final long minTracesStored;

  KafkaStorageHttpService(KafkaStorage storage) {
    this.storage = storage;
    this.minTracesStored = storage.minTracesStored;
  }

  @Get("/dependencies")
  public AggregatedHttpResponse getDependencies(
    @Param("endTs") long endTs,
    @Param("lookback") long lookback
  ) {
    try {
      if (!storage.dependencyQueryEnabled) return AggregatedHttpResponse.of(HttpStatus.NOT_FOUND);
      ReadOnlyWindowStore<Long, DependencyLink> store =
        storage.getDependencyStorageStream()
          .store(StoreQueryParameters.fromNameAndType(DEPENDENCIES_STORE_NAME,
            QueryableStoreTypes.windowStore()));
      List<DependencyLink> links = new ArrayList<>();
      Instant from = Instant.ofEpochMilli(endTs - lookback);
      Instant to = Instant.ofEpochMilli(endTs);
      try (KeyValueIterator<Windowed<Long>, DependencyLink> iterator = store.fetchAll(from, to)) {
        iterator.forEachRemaining(keyValue -> links.add(keyValue.value));
      }
      List<DependencyLink> mergedLinks = DependencyLinker.merge(links);
      LOG.debug("Dependencies found from={}-to={}: {}", from, to, mergedLinks.size());
      return AggregatedHttpResponse.of(
        HttpStatus.OK,
        MediaType.JSON,
        DependencyLinkBytesEncoder.JSON_V1.encodeList(mergedLinks));
    } catch (InvalidStateStoreException e) {
      LOG.debug("State store is not ready", e);
      return AggregatedHttpResponse.of(HttpStatus.SERVICE_UNAVAILABLE);
    }
  }

  @Get("/serviceNames")
  @ProducesJson
  public JsonNode getServiceNames() {
    try {
      if (!storage.traceSearchEnabled) return MAPPER.createArrayNode();
      ReadOnlyWindowStore<String, Set<String>> store = storage.getTraceStorageStream()
        .store(StoreQueryParameters.fromNameAndType(SPAN_NAMES_STORE_NAME,
          QueryableStoreTypes.windowStore()));
      ArrayNode array = MAPPER.createArrayNode();
      try (KeyValueIterator<Windowed<String>, Set<String>> all = store.backwardAll()) {
        all.forEachRemaining(keyValue -> array.add(keyValue.key.key()));
      }
      return array;
    } catch (InvalidStateStoreException e) {
      LOG.debug("State store is not ready", e);
      throw e;
    }
  }

  @Get("/serviceNames/:service_name/spanNames")
  @ProducesJson
  public JsonNode getSpanNames(@Param("service_name") String serviceName) {
    try {
      if (!storage.traceSearchEnabled) return MAPPER.createArrayNode();
      ReadOnlyWindowStore<String, Set<String>> store = storage.getTraceStorageStream()
        .store(StoreQueryParameters.fromNameAndType(SPAN_NAMES_STORE_NAME,
          QueryableStoreTypes.windowStore()));
      Instant to = Instant.now();
      Instant from = to.minus(Duration.ofDays(7));
      ArrayNode array = MAPPER.createArrayNode();
      try (WindowStoreIterator<Set<String>> all = store.backwardFetch(serviceName, from, to)) {
        if (all.hasNext()) {
          Set<String> names = all.next().value;
          if (names != null) names.forEach(array::add);
        }
      }
      return array;
    } catch (InvalidStateStoreException e) {
      LOG.debug("State store is not ready", e);
      throw e;
    }
  }

  @Get("/serviceNames/:service_name/remoteServiceNames")
  @ProducesJson
  public JsonNode getRemoteServiceNames(@Param("service_name") String serviceName) {
    try {
      if (!storage.traceSearchEnabled) return MAPPER.createArrayNode();
      ReadOnlyWindowStore<String, Set<String>> store = storage.getTraceStorageStream()
        .store(StoreQueryParameters.fromNameAndType(REMOTE_SERVICE_NAMES_STORE_NAME,
          QueryableStoreTypes.windowStore()));
      Instant to = Instant.now();
      Instant from = to.minus(Duration.ofDays(7));
      ArrayNode array = MAPPER.createArrayNode();
      try (WindowStoreIterator<Set<String>> all = store.backwardFetch(serviceName, from, to)) {
        if (all.hasNext()) {
          Set<String> names = all.next().value;
          if (names != null) names.forEach(array::add);
        }
      }
      return array;
    } catch (InvalidStateStoreException e) {
      LOG.debug("State store is not ready", e);
      throw e;
    }
  }

  @Get("/autocompleteTags/:key")
  @ProducesJson
  public JsonNode getAutocompleteValues(@Param("key") String key) {
    try {
      if (!storage.traceSearchEnabled) return MAPPER.createArrayNode();
      ReadOnlyWindowStore<String, Set<String>> store = storage.getTraceStorageStream()
        .store(StoreQueryParameters.fromNameAndType(AUTOCOMPLETE_TAGS_STORE_NAME,
          QueryableStoreTypes.windowStore()));
      Instant to = Instant.now();
      Instant from = to.minus(Duration.ofDays(7));
      ArrayNode array = MAPPER.createArrayNode();
      try (WindowStoreIterator<Set<String>> all = store.backwardFetch(key, from, to)) {
        if (all.hasNext()) {
          Set<String> names = all.next().value;
          if (names != null) names.forEach(array::add);
        }
      }
      return array;
    } catch (InvalidStateStoreException e) {
      LOG.debug("State store is not ready", e);
      throw e;
    }
  }

  @Get("/traces")
  public AggregatedHttpResponse getTraces(
    @Param("serviceName") Optional<String> serviceName,
    @Param("remoteServiceName") Optional<String> remoteServiceName,
    @Param("spanName") Optional<String> spanName,
    @Param("annotationQuery") Optional<String> annotationQuery,
    @Param("minDuration") Optional<Long> minDuration,
    @Param("maxDuration") Optional<Long> maxDuration,
    @Param("endTs") Optional<Long> endTs,
    @Default("86400000") @Param("lookback") Long lookback,
    @Default("10") @Param("limit") int limit
  ) {
    try {
      if (!storage.traceSearchEnabled) return AggregatedHttpResponse.of(HttpStatus.NOT_FOUND);
      QueryRequest request =
        QueryRequest.newBuilder()
          .serviceName(serviceName.orElse(null))
          .remoteServiceName(remoteServiceName.orElse(null))
          .spanName(spanName.orElse(null))
          .parseAnnotationQuery(annotationQuery.orElse(null))
          .minDuration(minDuration.orElse(null))
          .maxDuration(maxDuration.orElse(null))
          .endTs(endTs.orElse(System.currentTimeMillis()))
          .lookback(lookback)
          .limit(limit)
          .build();
      ReadOnlyWindowStore<String, List<Span>> tracesStore =
        storage.getTraceStorageStream().store(
          StoreQueryParameters.fromNameAndType(TRACES_STORE_NAME,
            QueryableStoreTypes.windowStore()));
      List<List<Span>> traces = new ArrayList<>();
      Instant from = Instant.ofEpochMilli(request.endTs() - request.lookback());
      Instant to = Instant.ofEpochMilli(request.endTs());
      try (
        KeyValueIterator<Windowed<String>, List<Span>> iterator = tracesStore.backwardFetchAll(from,
          to)) {
        while (iterator.hasNext()) {
          List<Span> spans = iterator.next().value;
          // apply filters
          if (request.test(spans)) traces.add(spans);
          if (traces.size() == request.limit()) break;
        }
      }
      traces.sort(Comparator.<List<Span>>comparingLong(o -> o.get(0).timestampAsLong()).reversed());
      LOG.debug("Traces found from query {}: {}", request, traces.size());
      List<List<Span>> result = traces.stream().limit(request.limit()).collect(Collectors.toList());
      return AggregatedHttpResponse.of(HttpStatus.OK, MediaType.JSON,
        writeTraces(result));
    } catch (InvalidStateStoreException e) {
      LOG.debug("State store is not ready", e);
      return AggregatedHttpResponse.of(HttpStatus.SERVICE_UNAVAILABLE);
    }
  }

  @Get("/traces/:trace_id")
  public AggregatedHttpResponse getTrace(@Param("trace_id") String traceId) {
    try {
      if (!storage.traceByIdQueryEnabled) return AggregatedHttpResponse.of(HttpStatus.NOT_FOUND);
      ReadOnlyWindowStore<String, List<Span>> store = storage.getTraceStorageStream()
        .store(StoreQueryParameters.fromNameAndType(TRACES_STORE_NAME,
          QueryableStoreTypes.windowStore()));
      Instant to = Instant.now();
      Instant from = to.minus(Duration.ofDays(1));
      List<Span> spans = new ArrayList<>();
      try (WindowStoreIterator<List<Span>> all = store.backwardFetch(traceId, from, to)) {
        if (all.hasNext()) {
          spans = all.next().value;
        }
      }
      return AggregatedHttpResponse.of(
        HttpStatus.OK,
        MediaType.JSON,
        SpanBytesEncoder.JSON_V2.encodeList(spans));
    } catch (InvalidStateStoreException e) {
      LOG.debug("State store is not ready", e);
      return AggregatedHttpResponse.of(HttpStatus.SERVICE_UNAVAILABLE);
    }
  }

  @Get("/traceMany")
  public AggregatedHttpResponse getTraces(@Param("traceIds") String traceIds) {
    try {
      if (!storage.traceByIdQueryEnabled) return AggregatedHttpResponse.of(HttpStatus.NOT_FOUND);
      ReadOnlyWindowStore<String, List<Span>> store = storage.getTraceStorageStream()
        .store(StoreQueryParameters.fromNameAndType(TRACES_STORE_NAME,
          QueryableStoreTypes.windowStore()));
      Instant to = Instant.now();
      Instant from = to.minus(Duration.ofDays(1));
      List<List<Span>> result = new ArrayList<>();
      for (String traceId : traceIds.split(",", 1000)) {
        try (WindowStoreIterator<List<Span>> all = store.backwardFetch(traceId, from, to)) {
          if (all.hasNext()) {
            result.add(all.next().value);
          }
        }
      }
      return AggregatedHttpResponse.of(HttpStatus.OK, MediaType.JSON, writeTraces(result));
    } catch (InvalidStateStoreException e) {
      LOG.debug("State store is not ready", e);
      return AggregatedHttpResponse.of(HttpStatus.SERVICE_UNAVAILABLE);
    }
  }

  @Get("/autocompleteTags")
  @ProducesJson
  public JsonNode getAutocompleteTags() {
    try {
      if (!storage.traceSearchEnabled) return MAPPER.createArrayNode();
      ReadOnlyKeyValueStore<String, Set<String>> autocompleteTagsStore =
        storage.getTraceStorageStream().store(StoreQueryParameters.fromNameAndType(AUTOCOMPLETE_TAGS_STORE_NAME,
          QueryableStoreTypes.keyValueStore()));
      ArrayNode array = MAPPER.createArrayNode();
      try (KeyValueIterator<String, Set<String>> all = autocompleteTagsStore.all()) {
        all.forEachRemaining(keyValue -> array.add(keyValue.key));
      }
      return array;
    } catch (InvalidStateStoreException e) {
      LOG.debug("State store is not ready", e);
      throw e;
    }
  }

  @Get("/instances/:store_name")
  @ProducesJson
  public KafkaStreamsMetadata getInstancesByStore(@Param("store_name") String storeName) {
    Collection<StreamsMetadata> metadata =
      storage.getTraceStorageStream().allMetadataForStore(storeName);
    metadata.addAll(storage.getDependencyStorageStream().allMetadataForStore(storeName));
    return KafkaStreamsMetadata.create(metadata);
  }

  @Get("/instances")
  @ProducesJson
  public KafkaStreamsMetadata getInstances() {
    Collection<StreamsMetadata> metadata = storage.getTraceStorageStream().allMetadata();
    metadata.addAll(storage.getDependencyStorageStream().allMetadata());
    return KafkaStreamsMetadata.create(metadata);
  }

  //Copy-paste from ZipkinQueryApiV2
  static byte[] writeTraces(List<List<Span>> traces) {
    // Get the encoded size of the nested list so that we don't need to grow the buffer
    int length = traces.size();
    int sizeInBytes = 2; // []
    if (length > 1) sizeInBytes += length - 1; // comma to join elements

    for (List<Span> spans : traces) {
      int jLength = spans.size();
      sizeInBytes += 2; // []
      if (jLength > 1) sizeInBytes += jLength - 1; // comma to join elements
      for (Span span : spans) {
        sizeInBytes += SpanBytesEncoder.JSON_V2.sizeInBytes(span);
      }
    }

    byte[] out = new byte[sizeInBytes];
    int pos = 0;
    out[pos++] = '['; // start list of traces
    for (int i = 0; i < length; i++) {
      pos += SpanBytesEncoder.JSON_V2.encodeList(traces.get(i), out, pos);
      if (i + 1 < length) out[pos++] = ',';
    }
    out[pos] = ']'; // stop list of traces
    return out;
  }
}
