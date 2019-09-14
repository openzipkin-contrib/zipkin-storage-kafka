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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.annotation.Default;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.ProducesJson;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.codec.DependencyLinkBytesEncoder;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.internal.DependencyLinker;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.kafka.streams.KafkaStreamsMetadata;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static zipkin2.storage.kafka.streams.DependencyStoreTopologySupplier.DEPENDENCIES_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStoreTopologySupplier.AUTOCOMPLETE_TAGS_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStoreTopologySupplier.REMOTE_SERVICE_NAMES_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStoreTopologySupplier.SERVICE_NAMES_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStoreTopologySupplier.SPAN_IDS_BY_TS_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStoreTopologySupplier.SPAN_NAMES_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStoreTopologySupplier.TRACES_STORE_NAME;

/**
 * Server to enable access to local stores.
 * <p>
 * Given the partitioned nature of local stores, a RPC layer is required to allow accessing
 * distributed state. This component exposes access to local state via Http call from {@link
 * KafkaSpanStore}
 *
 * @since 0.6.0
 */
public class KafkaStoreHttpService implements Consumer<ServerBuilder> {
  static final Logger LOG = LoggerFactory.getLogger(KafkaStoreHttpService.class);
  static final ObjectMapper MAPPER = new ObjectMapper();

  final KafkaStorage storage;
  final long minTracesStored;

  KafkaStoreHttpService(KafkaStorage storage) {
    this.storage = storage;
    this.minTracesStored = storage.minTracesStored;
  }

  @Override public void accept(ServerBuilder serverBuilder) {
    serverBuilder.annotatedService("/zipkin/storage/kafka", this);
  }

  @Get("/dependencies")
  public AggregatedHttpResponse getDependencies(
      @Param("endTs") long endTs,
      @Param("lookback") long lookback) {
    try {
      ReadOnlyWindowStore<Long, DependencyLink> dependenciesStore =
          storage.getDependencyStoreStream().store(DEPENDENCIES_STORE_NAME,
              QueryableStoreTypes.windowStore());
      List<DependencyLink> links = new ArrayList<>();
      Instant from = Instant.ofEpochMilli(endTs - lookback);
      Instant to = Instant.ofEpochMilli(endTs);
      dependenciesStore.fetchAll(from, to)
          .forEachRemaining(keyValue -> links.add(keyValue.value));
      List<DependencyLink> mergedLinks = DependencyLinker.merge(links);
      LOG.debug("Dependencies found from={}-to={}: {}", from, to, mergedLinks.size());
      return AggregatedHttpResponse.of(
          HttpStatus.OK,
          MediaType.JSON,
          DependencyLinkBytesEncoder.JSON_V1.encodeList(mergedLinks));
    } catch (InvalidStateStoreException e) {
      LOG.warn("State store is not ready", e);
      return AggregatedHttpResponse.of(HttpStatus.SERVICE_UNAVAILABLE);
    }
  }

  @Get("/serviceNames")
  @ProducesJson
  public JsonNode getServiceNames() {
    try {
      ReadOnlyKeyValueStore<String, String> store =
          storage.getTraceStoreStream()
              .store(SERVICE_NAMES_STORE_NAME, QueryableStoreTypes.keyValueStore());
      ArrayNode array = MAPPER.createArrayNode();
      store.all().forEachRemaining(keyValue -> array.add(keyValue.value));
      return array;
    } catch (InvalidStateStoreException e) {
      LOG.warn("State store is not ready", e);
      throw e;
    }
  }

  @Get("/serviceNames/:service_name/spanNames")
  @ProducesJson
  public JsonNode getSpanNames(@Param("service_name") String serviceName) {
    try {
      ReadOnlyKeyValueStore<String, Set<String>> store =
          storage.getTraceStoreStream()
              .store(SPAN_NAMES_STORE_NAME, QueryableStoreTypes.keyValueStore());
      Set<String> names = store.get(serviceName);
      ArrayNode array = MAPPER.createArrayNode();
      names.forEach(array::add);
      return array;
    } catch (InvalidStateStoreException e) {
      LOG.warn("State store is not ready", e);
      throw e;
    }
  }

  @Get("/serviceNames/:service_name/remoteServiceNames")
  @ProducesJson
  public JsonNode getRemoteServiceNames(@Param("service_name") String serviceName) {
    try {
      ReadOnlyKeyValueStore<String, Set<String>> store =
          storage.getTraceStoreStream().store(REMOTE_SERVICE_NAMES_STORE_NAME,
              QueryableStoreTypes.keyValueStore());
      Set<String> names = store.get(serviceName);
      ArrayNode array = MAPPER.createArrayNode();
      names.forEach(array::add);
      return (array);
    } catch (InvalidStateStoreException e) {
      LOG.warn("State store is not ready", e);
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
      @Default("10") @Param("limit") int limit) {
    try {
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
      ReadOnlyKeyValueStore<String, List<Span>> tracesStore =
          storage.getTraceStoreStream()
              .store(TRACES_STORE_NAME, QueryableStoreTypes.keyValueStore());
      ReadOnlyKeyValueStore<Long, Set<String>> traceIdsByTsStore =
          storage.getTraceStoreStream().store(SPAN_IDS_BY_TS_STORE_NAME,
              QueryableStoreTypes.keyValueStore());
      List<List<Span>> traces = new ArrayList<>();
      List<String> traceIds = new ArrayList<>();
      long from = MILLISECONDS.toMicros(request.endTs() - request.lookback());
      long to = MILLISECONDS.toMicros(request.endTs());
      long bucket = SECONDS.toMicros(30);
      long checkpoint = to - bucket; // 30 sec before upper bound
      if (checkpoint <= from ||
          tracesStore.approximateNumEntries() <= minTracesStored) { // do one run
        try (KeyValueIterator<Long, Set<String>> spanIds = traceIdsByTsStore.range(from, to)) {
          addResults(request, tracesStore, traces, traceIds, spanIds);
        }
      } else {
        while (checkpoint > from && traces.size() < request.limit()) {
          try (KeyValueIterator<Long, Set<String>> spanIds =
                   traceIdsByTsStore.range(checkpoint, to)) {
            addResults(request, tracesStore, traces, traceIds, spanIds);
          }
          to = checkpoint;
          checkpoint = checkpoint - bucket; // 1 min before more
        }
      }
      traces.sort(Comparator.<List<Span>>comparingLong(o -> o.get(0).timestampAsLong()).reversed());
      LOG.debug("Traces found from query {}: {}", request, traces.size());
      List<List<Span>> result = traces.stream().limit(request.limit()).collect(Collectors.toList());
      return AggregatedHttpResponse.of(HttpStatus.OK, MediaType.JSON,
          writeTraces(result));
    } catch (InvalidStateStoreException e) {
      LOG.warn("State store is not ready", e);
      return AggregatedHttpResponse.of(HttpStatus.SERVICE_UNAVAILABLE);
    }
  }

  void addResults(
      QueryRequest request,
      ReadOnlyKeyValueStore<String, List<Span>> tracesStore,
      List<List<Span>> traces,
      List<String> traceIds,
      KeyValueIterator<Long, Set<String>> spanIds) {
    spanIds.forEachRemaining(keyValue -> {
      for (String traceId : keyValue.value) {
        if (!traceIds.contains(traceId)) {
          List<Span> spans = tracesStore.get(traceId);
          if (spans != null && !spans.isEmpty() && request.test(spans)) { // apply filters
            traceIds.add(traceId); // adding to check if we have already add it later
            traces.add(spans);
          }
        }
      }
    });
  }

  @Get("/traces/:trace_id")
  public AggregatedHttpResponse getTrace(@Param("trace_id") String traceId) {
    try {
      ReadOnlyKeyValueStore<String, List<Span>> store =
          storage.getTraceStoreStream()
              .store(TRACES_STORE_NAME, QueryableStoreTypes.keyValueStore());
      List<Span> spans = store.get(traceId);
      return AggregatedHttpResponse.of(
          HttpStatus.OK,
          MediaType.JSON,
          SpanBytesEncoder.JSON_V2.encodeList(spans));
    } catch (InvalidStateStoreException e) {
      LOG.warn("State store is not ready", e);
      return AggregatedHttpResponse.of(HttpStatus.SERVICE_UNAVAILABLE);
    }
  }

  @Get("/autocompleteTags")
  @ProducesJson
  public JsonNode getAutocompleteTags() {
    try {
      ReadOnlyKeyValueStore<String, Set<String>> autocompleteTagsStore =
          storage.getTraceStoreStream().store(AUTOCOMPLETE_TAGS_STORE_NAME,
              QueryableStoreTypes.keyValueStore());
      ArrayNode array = MAPPER.createArrayNode();
      autocompleteTagsStore.all().forEachRemaining(keyValue -> array.add(keyValue.key));
      return array;
    } catch (InvalidStateStoreException e) {
      LOG.warn("State store is not ready", e);
      throw e;
    }
  }

  @Get("/autocompleteTags/:key")
  @ProducesJson
  public JsonNode getAutocompleteValues(@Param("key") String key) {
    try {
      ReadOnlyKeyValueStore<String, Set<String>> autocompleteTagsStore =
          storage.getTraceStoreStream().store(AUTOCOMPLETE_TAGS_STORE_NAME,
              QueryableStoreTypes.keyValueStore());
      Set<String> valuesSet = autocompleteTagsStore.get(key);
      if (valuesSet == null) valuesSet = new HashSet<>();
      ArrayNode array = MAPPER.createArrayNode();
      valuesSet.forEach(array::add);
      return array;
    } catch (InvalidStateStoreException e) {
      LOG.warn("State store is not ready", e);
      throw e;
    }
  }

  @Get("/instances/:store_name")
  @ProducesJson
  public KafkaStreamsMetadata getInstancesByStore(@Param("store_name") String storeName) {
    Collection<StreamsMetadata> metadata =
        storage.getTraceStoreStream().allMetadataForStore(storeName);
    metadata.addAll(storage.getDependencyStoreStream().allMetadataForStore(storeName));
    return KafkaStreamsMetadata.create(metadata);
  }

  @Get("/instances")
  @ProducesJson
  public KafkaStreamsMetadata getInstances() {
    Collection<StreamsMetadata> metadata = storage.getTraceStoreStream().allMetadata();
    metadata.addAll(storage.getDependencyStoreStream().allMetadata());
    return KafkaStreamsMetadata.create(metadata);
  }

  //Copy-paste from ZipkinQueryApiV2
  static byte[] writeTraces(List<List<Span>> traces) {
    // Get the encoded size of the nested list so that we don't need to grow the buffer
    int length = traces.size();
    int sizeInBytes = 2; // []
    if (length > 1) sizeInBytes += length - 1; // comma to join elements

    for (int i = 0; i < length; i++) {
      List<zipkin2.Span> spans = traces.get(i);
      int jLength = spans.size();
      sizeInBytes += 2; // []
      if (jLength > 1) sizeInBytes += jLength - 1; // comma to join elements
      for (int j = 0; j < jLength; j++) {
        sizeInBytes += SpanBytesEncoder.JSON_V2.sizeInBytes(spans.get(j));
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
