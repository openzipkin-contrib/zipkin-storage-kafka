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
package zipkin2.storage.kafka.streams;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.storage.kafka.streams.serdes.DependencyLinkSerde;
import zipkin2.storage.kafka.streams.serdes.NamesSerde;
import zipkin2.storage.kafka.streams.serdes.SpanIdsSerde;
import zipkin2.storage.kafka.streams.serdes.SpansSerde;

/**
 * Aggregation and storage of spans into traces.
 */
public class TraceStoreSupplier implements Supplier<Topology> {
  public static final String TRACES_STORE_NAME = "zipkin-traces";
  public static final String SPAN_IDS_BY_TS_STORE_NAME = "zipkin-traces-by-timestamp";
  public static final String SERVICE_NAMES_STORE_NAME = "zipkin-service-names";
  public static final String SPAN_NAMES_STORE_NAME = "zipkin-span-names";
  public static final String REMOTE_SERVICE_NAMES_STORE_NAME = "zipkin-remote-service-names";
  public static final String DEPENDENCY_LINKS_STORE_NAME = "zipkin_dependency_links";
  public static final String AUTOCOMPLETE_TAGS_STORE_NAME = "zipkin-autocomplete-tags";

  static final Logger LOG = LoggerFactory.getLogger(TraceStoreSupplier.class);
  // Kafka topics
  final String tracesTopic;
  final String dependencyLinksTopic;
  // Limits
  final List<String> autocompleteKeys;
  final Duration tracesRetentionScanFrequency;
  final Duration tracesRetentionPeriod;
  final Duration dependenciesRetentionPeriod;
  final Duration dependenciesWindowSize;
  // SerDes
  final SpansSerde spansSerde;
  final SpanIdsSerde spanIdsSerde;
  final NamesSerde namesSerde;
  final DependencyLinkSerde dependencyLinkSerde;

  public TraceStoreSupplier(String tracesTopic, String dependencyLinksTopic,
      List<String> autocompleteKeys, Duration tracesRetentionScanFrequency,
      Duration tracesRetentionPeriod,
      Duration dependenciesRetentionPeriod, Duration dependenciesWindowSize) {
    this.tracesTopic = tracesTopic;
    this.dependencyLinksTopic = dependencyLinksTopic;
    this.autocompleteKeys = autocompleteKeys;
    this.tracesRetentionScanFrequency = tracesRetentionScanFrequency;
    this.tracesRetentionPeriod = tracesRetentionPeriod;
    this.dependenciesRetentionPeriod = dependenciesRetentionPeriod;
    this.dependenciesWindowSize = dependenciesWindowSize;
    spansSerde = new SpansSerde();
    spanIdsSerde = new SpanIdsSerde();
    namesSerde = new NamesSerde();
    dependencyLinkSerde = new DependencyLinkSerde();
  }

  @Override public Topology get() {
    StreamsBuilder builder = new StreamsBuilder();

    builder
        .addStateStore(Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(TRACES_STORE_NAME),
            Serdes.String(),
            spansSerde
        ))
        .addStateStore(Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(SPAN_IDS_BY_TS_STORE_NAME),
            Serdes.Long(),
            spanIdsSerde
        ))
        .addStateStore(Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(SERVICE_NAMES_STORE_NAME),
            Serdes.String(),
            Serdes.String()
        ))
        .addStateStore(Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(SPAN_NAMES_STORE_NAME),
            Serdes.String(),
            namesSerde
        ))
        .addStateStore(Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(REMOTE_SERVICE_NAMES_STORE_NAME),
            Serdes.String(),
            namesSerde
        ))
        .addStateStore(Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(AUTOCOMPLETE_TAGS_STORE_NAME),
            Serdes.String(),
            namesSerde
        ));

    // Traces stream
    KStream<String, List<Span>> stream = builder
        .stream(tracesTopic, Consumed.with(Serdes.String(), spansSerde));
    // Store traces
    stream
        .process(() -> new Processor<String, List<Span>>() {
          ProcessorContext context;
          // Actual traces store
          KeyValueStore<String, List<Span>> tracesStore;
          // timestamp index for trace IDs
          KeyValueStore<Long, Set<String>> spanIdsByTsStore;

          @Override public void init(ProcessorContext context) {
            this.context = context;
            tracesStore =
                (KeyValueStore<String, List<Span>>) context.getStateStore(TRACES_STORE_NAME);
            spanIdsByTsStore =
                (KeyValueStore<Long, Set<String>>) context.getStateStore(SPAN_IDS_BY_TS_STORE_NAME);
            // Retention scheduling
            context.schedule(
                tracesRetentionScanFrequency,
                PunctuationType.STREAM_TIME,
                timestamp -> {
                  // preparing range filtering
                  long from = 0L;
                  long to = timestamp - tracesRetentionPeriod.toMillis();
                  long toMicro = to * 1000;
                  // query traceIds active during period
                  try (final KeyValueIterator<Long, Set<String>> all =
                           spanIdsByTsStore.range(from, toMicro)) {
                    int deletions = 0; // logging purpose
                    while (all.hasNext()) {
                      final KeyValue<Long, Set<String>> record = all.next();
                      spanIdsByTsStore.delete(record.key); // clean timestamp index
                      for (String traceId : record.value) {
                        tracesStore.delete(traceId); // clean traces store
                        deletions++;
                      }
                    }
                    if (deletions > 0) {
                      LOG.info("Traces deletion emitted: {}, older than {}",
                          deletions,
                          Instant.ofEpochMilli(to).atZone(ZoneId.systemDefault()));
                    }
                  }
                });
          }

          @Override public void process(String traceId, List<Span> spans) {
            if (!spans.isEmpty()) {
              // Persist traces
              List<Span> currentSpans = tracesStore.get(traceId);
              if (currentSpans == null) {
                currentSpans = new ArrayList<>();
              }
              currentSpans.addAll(spans);
              tracesStore.put(traceId, currentSpans);
              // Persist timestamp indexed span ids
              long timestamp = spans.get(0).timestamp();
              Set<String> currentSpanIds = spanIdsByTsStore.get(timestamp);
              if (currentSpanIds == null) {
                currentSpanIds = new HashSet<>();
              }
              currentSpanIds.add(traceId);
              spanIdsByTsStore.put(timestamp, currentSpanIds);
            }
          }

          @Override public void close() {
          }
        }, TRACES_STORE_NAME, SPAN_IDS_BY_TS_STORE_NAME);
    // Store service, span and remote service names
    stream.process(() -> new Processor<String, List<Span>>() {
          KeyValueStore<String, String> serviceNameStore;
          KeyValueStore<String, Set<String>> spanNamesStore;
          KeyValueStore<String, Set<String>> remoteServiceNamesStore;
          KeyValueStore<String, Set<String>> autocompleteTagsStore;

          @Override
          public void init(ProcessorContext context) {
            serviceNameStore =
                (KeyValueStore<String, String>) context.getStateStore(SERVICE_NAMES_STORE_NAME);
            spanNamesStore =
                (KeyValueStore<String, Set<String>>) context.getStateStore(SPAN_NAMES_STORE_NAME);
            remoteServiceNamesStore =
                (KeyValueStore<String, Set<String>>) context.getStateStore(
                    REMOTE_SERVICE_NAMES_STORE_NAME);
            autocompleteTagsStore =
                (KeyValueStore<String, Set<String>>) context.getStateStore(
                    AUTOCOMPLETE_TAGS_STORE_NAME);
          }

          @Override
          public void process(String traceId, List<Span> spans) {
            for (Span span : spans) {
              if (span.localServiceName() != null) { // if service name
                serviceNameStore.putIfAbsent(span.localServiceName(),
                    span.localServiceName()); // store it
                if (span.name() != null) { // store span names
                  Set<String> spanNames = spanNamesStore.get(span.localServiceName());
                  if (spanNames == null) spanNames = new HashSet<>();
                  spanNames.add(span.name());
                  spanNamesStore.put(span.localServiceName(), spanNames);
                }
                if (span.remoteServiceName() != null) { // store remote service names
                  Set<String> remoteServiceNames = remoteServiceNamesStore.get(span.localServiceName());
                  if (remoteServiceNames == null) remoteServiceNames = new HashSet<>();
                  remoteServiceNames.add(span.remoteServiceName());
                  remoteServiceNamesStore.put(span.localServiceName(), remoteServiceNames);
                }
              }
              if (!span.tags().isEmpty()) {
                span.tags().forEach((key, value) -> {
                  if (autocompleteKeys.contains(key)) {
                    Set<String> values = autocompleteTagsStore.get(key);
                    if (values == null) values = new HashSet<>();
                    values.add(value);
                    autocompleteTagsStore.put(key, values);
                  }
                });
              }
            }
          }

          @Override public void close() {
          }
        },
        SERVICE_NAMES_STORE_NAME,
        SPAN_NAMES_STORE_NAME,
        REMOTE_SERVICE_NAMES_STORE_NAME,
        AUTOCOMPLETE_TAGS_STORE_NAME);

    // Dependency links window store
    builder.addStateStore(Stores.windowStoreBuilder(
        Stores.persistentWindowStore(
            DEPENDENCY_LINKS_STORE_NAME,
            dependenciesRetentionPeriod,
            dependenciesWindowSize,
            false),
        Serdes.String(),
        dependencyLinkSerde
    ));
    // Consume dependency links stream
    builder.stream(dependencyLinksTopic, Consumed.with(Serdes.String(), dependencyLinkSerde))
        // Storage
        .process(() -> new Processor<String, DependencyLink>() {
          ProcessorContext context;
          WindowStore<String, DependencyLink> dependencyLinksStore;

          @Override
          public void init(ProcessorContext context) {
            this.context = context;
            dependencyLinksStore =
                (WindowStore<String, DependencyLink>) context.getStateStore(
                    DEPENDENCY_LINKS_STORE_NAME);
          }

          @Override
          public void process(String linkKey, DependencyLink link) {
            // Event time
            Instant now = Instant.ofEpochMilli(context.timestamp());
            Instant from = now.minus(dependenciesWindowSize);
            WindowStoreIterator<DependencyLink> currentLinkWindow =
                dependencyLinksStore.fetch(linkKey, from, now);
            // Get latest window. Only two are possible.
            KeyValue<Long, DependencyLink> windowAndValue = null;
            if (currentLinkWindow.hasNext()) windowAndValue = currentLinkWindow.next();
            if (currentLinkWindow.hasNext()) windowAndValue = currentLinkWindow.next();
            // Persist dependency link per window
            if (windowAndValue != null) {
              DependencyLink currentLink = windowAndValue.value;
              DependencyLink build = currentLink.toBuilder()
                  .callCount(currentLink.callCount() + link.callCount())
                  .errorCount(currentLink.errorCount() + link.errorCount())
                  .build();
              dependencyLinksStore.put(
                  linkKey,
                  build,
                  windowAndValue.key);
            } else {
              dependencyLinksStore.put(linkKey, link);
            }
          }

          @Override
          public void close() {
          }
        }, DEPENDENCY_LINKS_STORE_NAME);

    return builder.build();
  }
}
