/*
 * Copyright 2019-2021 The OpenZipkin Authors
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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashSet;
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
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import zipkin2.Span;
import zipkin2.storage.kafka.streams.serdes.NamesSerde;
import zipkin2.storage.kafka.streams.serdes.SpanIdsSerde;
import zipkin2.storage.kafka.streams.serdes.SpansSerde;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

/**
 * Storage of Traces, Service names and Autocomplete Tags.
 */
public class TraceStorageTopology implements Supplier<Topology> {
  public static final String TRACES_STORE_NAME = "zipkin-traces";
  public static final String SPAN_NAMES_STORE_NAME = "zipkin-span-names";
  public static final String REMOTE_SERVICE_NAMES_STORE_NAME = "zipkin-remote-service-names";
  public static final String AUTOCOMPLETE_TAGS_STORE_NAME = "zipkin-autocomplete-tags";

  // Kafka topics
  final String spansTopic;
  // Limits
  final List<String> autoCompleteKeys;
  final Duration traceTtl;
  final Duration traceTtlCheckInterval;
  final long minTracesStored;
  // Flags
  final boolean traceSearchEnabled;
  final boolean traceByIdQueryEnabled;
  // SerDes
  final SpansSerde spansSerde;
  final SpanIdsSerde spanIdsSerde;
  final NamesSerde namesSerde;

  final Counter brokenTracesTotal;

  public TraceStorageTopology(
    String spansTopic,
    List<String> autoCompleteKeys,
    Duration traceTtl,
    Duration traceTtlCheckInterval,
    long minTracesStored,
    boolean traceByIdQueryEnabled,
    boolean traceSearchEnabled
  ) {
    this.spansTopic = spansTopic;
    this.autoCompleteKeys = autoCompleteKeys;
    this.traceTtl = traceTtl;
    this.traceTtlCheckInterval = traceTtlCheckInterval;
    this.minTracesStored = minTracesStored;
    this.traceByIdQueryEnabled = traceByIdQueryEnabled;
    this.traceSearchEnabled = traceSearchEnabled;
    spansSerde = new SpansSerde();
    spanIdsSerde = new SpanIdsSerde();
    namesSerde = new NamesSerde();
    brokenTracesTotal = Metrics.counter("zipkin.storage.kafka.traces.broken");
  }

  @Override public Topology get() {
    StreamsBuilder builder = new StreamsBuilder();
    if (traceSearchEnabled || traceByIdQueryEnabled) {
      builder
        // Logging disabled to avoid long starting times, with logging disabled to process incoming
        // spans since last restart
        .addStateStore(Stores.windowStoreBuilder(
          Stores.persistentWindowStore(TRACES_STORE_NAME, Duration.ofDays(1), Duration.ofHours(1),
            false),
          Serdes.String(),
          spansSerde).withLoggingDisabled());
      // Traces stream
      KStream<String, List<Span>> spansStream = builder
        .stream(spansTopic, Consumed.with(Serdes.String(), spansSerde));
      // Store traces
      spansStream.process(() -> new Processor<String, List<Span>>() {
        // Actual traces store
        WindowStore<String, List<Span>> tracesStore;

        @Override public void init(ProcessorContext context) {
          tracesStore = context.getStateStore(TRACES_STORE_NAME);
        }

        @Override public void process(String traceId, List<Span> spans) {
          if (!spans.isEmpty()) {
            // Persist traces
            final Instant now = Instant.now();
            try (WindowStoreIterator<List<Span>> iterator =
                   tracesStore.backwardFetch(traceId,
                     now.minusMillis(Duration.ofDays(1).toMillis()), now)) {
              if (iterator.hasNext()) {
                KeyValue<Long, List<Span>> current = iterator.next();
                current.value.addAll(spans);
                tracesStore.put(traceId, current.value, current.key);
              } else {
                long timestamp = MICROSECONDS.toMillis(spans.get(0).timestamp());
                tracesStore.put(traceId, spans, timestamp);
              }
            }
          }
        }

        @Override
        public void close() {
        }
      }, TRACES_STORE_NAME);
      if (traceSearchEnabled) {
        builder
          // In-memory as span names are bounded, with logging enabled to build state
          // with all values collected
          .addStateStore(Stores.windowStoreBuilder(
            Stores.inMemoryWindowStore(SPAN_NAMES_STORE_NAME, Duration.ofDays(7),
              Duration.ofDays(1), false),
            Serdes.String(),
            namesSerde))
          // In-memory as remote-service names are bounded, with logging enabled to build state
          // with all values collected
          .addStateStore(Stores.windowStoreBuilder(
            Stores.inMemoryWindowStore(REMOTE_SERVICE_NAMES_STORE_NAME, Duration.ofDays(7),
              Duration.ofDays(1), false),
            Serdes.String(),
            namesSerde))
          // Persistent as values could be unbounded, but with logging enabled to build state
          // with all values collected
          .addStateStore(Stores.windowStoreBuilder(
            Stores.persistentWindowStore(AUTOCOMPLETE_TAGS_STORE_NAME, Duration.ofDays(7),
              Duration.ofDays(1), false),
            Serdes.String(),
            namesSerde));
        // Store service, span and remote service names
        spansStream.process(() -> new Processor<String, List<Span>>() {
            WindowStore<String, Set<String>> spanNamesStore;
            WindowStore<String, Set<String>> remoteServiceNamesStore;
            WindowStore<String, Set<String>> tagsStore;

            @Override
            public void init(ProcessorContext context) {
              spanNamesStore = context.getStateStore(SPAN_NAMES_STORE_NAME);
              remoteServiceNamesStore = context.getStateStore(REMOTE_SERVICE_NAMES_STORE_NAME);
              tagsStore = context.getStateStore(AUTOCOMPLETE_TAGS_STORE_NAME);
            }

            @Override
            public void process(String traceId, List<Span> spans) {
              Instant now = Instant.now();
              for (Span span : spans) {
                final long timestamp = MICROSECONDS.toMillis(span.timestamp());
                if (span.localServiceName() != null) { // if service name
                  if (span.name() != null) { // store span names
                    try (WindowStoreIterator<Set<String>> iterator =
                           spanNamesStore.backwardFetch(span.localServiceName(),
                             now.minusMillis(Duration.ofDays(7).toMillis()), now)) {
                      if (iterator.hasNext()) {
                        KeyValue<Long, Set<String>> current = iterator.next();
                        current.value.add(span.name());
                        if (!current.value.contains(span.name())) {
                          spanNamesStore.put(span.localServiceName(), current.value,
                            timestamp);
                        }
                      } else {
                        Set<String> values = new LinkedHashSet<>();
                        values.add(span.name());
                        spanNamesStore.put(span.localServiceName(), values, timestamp);
                      }
                    }
                  }
                  if (span.remoteServiceName() != null) { // store remote service names
                    try (WindowStoreIterator<Set<String>> iterator =
                           remoteServiceNamesStore.backwardFetch(span.localServiceName(),
                             now.minusMillis(Duration.ofDays(7).toMillis()), now)) {
                      if (iterator.hasNext()) {
                        KeyValue<Long, Set<String>> current = iterator.next();
                        if (!current.value.contains(span.remoteServiceName())) {
                          current.value.add(span.remoteServiceName());
                          remoteServiceNamesStore.put(span.localServiceName(), current.value,
                            timestamp);
                        }
                      } else {
                        Set<String> values = new LinkedHashSet<>();
                        values.add(span.remoteServiceName());
                        remoteServiceNamesStore.put(span.localServiceName(), values,
                          timestamp);
                      }
                    }
                  }
                }
                if (!span.tags().isEmpty()) {
                  autoCompleteKeys.forEach(tagKey -> {
                    String value = span.tags().get(tagKey);
                    if (value != null) {
                      try (WindowStoreIterator<Set<String>> iterator =
                             tagsStore.backwardFetch(tagKey,
                               now.minusMillis(Duration.ofDays(7).toMillis()), now)) {
                        if (iterator.hasNext()) {
                          KeyValue<Long, Set<String>> current = iterator.next();
                          if (!current.value.contains(value)) {
                            current.value.add(value);
                            tagsStore.put(tagKey, current.value, timestamp);
                          }
                        } else {
                          Set<String> values = new LinkedHashSet<>();
                          values.add(value);
                          tagsStore.put(tagKey, values, timestamp);
                        }
                      }
                    }
                  });
                }
              }
            }

            @Override public void close() {
            }
          },
          SPAN_NAMES_STORE_NAME,
          REMOTE_SERVICE_NAMES_STORE_NAME,
          AUTOCOMPLETE_TAGS_STORE_NAME);
      }
    }
    return builder.build();
  }
}
