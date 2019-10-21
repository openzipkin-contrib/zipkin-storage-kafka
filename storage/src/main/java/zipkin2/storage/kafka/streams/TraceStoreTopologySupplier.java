/*
 * Copyright 2019 The OpenZipkin Authors
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
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serdes;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zipkin2.Span;
import zipkin2.storage.kafka.streams.serdes.NamesSerde;
import zipkin2.storage.kafka.streams.serdes.SpanIdsSerde;
import zipkin2.storage.kafka.streams.serdes.SpansSerde;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Storage of Traces, Service names and Autocomplete Tags.
 */
public class TraceStoreTopologySupplier implements Supplier<Topology> {
  public static final String TRACES_STORE_NAME = "zipkin-traces";
  public static final String SPAN_IDS_BY_TS_STORE_NAME = "zipkin-traces-by-timestamp";
  public static final String SERVICE_NAMES_STORE_NAME = "zipkin-service-names";
  public static final String SPAN_NAMES_STORE_NAME = "zipkin-span-names";
  public static final String REMOTE_SERVICE_NAMES_STORE_NAME = "zipkin-remote-service-names";
  public static final String AUTOCOMPLETE_TAGS_STORE_NAME = "zipkin-autocomplete-tags";
  static final Logger LOG = LogManager.getLogger();

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

  public TraceStoreTopologySupplier(
      String spansTopic,
      List<String> autoCompleteKeys,
      Duration traceTtl,
      Duration traceTtlCheckInterval,
      long minTracesStored,
      boolean traceByIdQueryEnabled,
      boolean traceSearchEnabled) {
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
          .addStateStore(Stores.keyValueStoreBuilder(
              Stores.persistentKeyValueStore(TRACES_STORE_NAME),
              Serdes.String(),
              spansSerde).withLoggingDisabled())
          // Disabling logging to avoid long starting times, with logging disabled to process incoming
          // spans since last restart
          .addStateStore(Stores.keyValueStoreBuilder(
              Stores.persistentKeyValueStore(SPAN_IDS_BY_TS_STORE_NAME),
              Serdes.Long(),
              spanIdsSerde).withLoggingDisabled());
      // Traces stream
      KStream<String, List<Span>> spansStream = builder
          .stream(spansTopic, Consumed.with(Serdes.String(), spansSerde));
      // Store traces
      spansStream.process(() -> new Processor<String, List<Span>>() {
        // Actual traces store
        KeyValueStore<String, List<Span>> tracesStore;
        // timestamp index for trace IDs
        KeyValueStore<Long, Set<String>> spanIdsByTsStore;

        @SuppressWarnings("unchecked")
        @Override public void init(ProcessorContext context) {
          tracesStore =
              (KeyValueStore<String, List<Span>>) context.getStateStore(TRACES_STORE_NAME);
          spanIdsByTsStore =
              (KeyValueStore<Long, Set<String>>) context.getStateStore(SPAN_IDS_BY_TS_STORE_NAME);
          // Retention scheduling
          context.schedule(
              traceTtlCheckInterval,
              PunctuationType.STREAM_TIME,
              timestamp -> {
                if (traceTtl.toMillis() > 0 &&
                    tracesStore.approximateNumEntries() > minTracesStored) {
                  // query traceIds active during period
                  long from = 0L;
                  long to = timestamp - traceTtl.toMillis();
                  try (final KeyValueIterator<Long, Set<String>> range =
                           spanIdsByTsStore.range(from, MILLISECONDS.toMicros(to))) {
                    range.forEachRemaining(record -> {
                      spanIdsByTsStore.delete(record.key); // clean timestamp index
                      for (String traceId : record.value) {
                        tracesStore.delete(traceId); // clean traces store
                      }
                    });
                    LOG.debug(
                        "Traces deletion emitted at {}, approx. number of traces stored {}",
                        Instant.ofEpochMilli(to).atZone(ZoneId.systemDefault()),
                        tracesStore.approximateNumEntries());
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
            } else {
              brokenTracesTotal.increment();
            }
            currentSpans.addAll(spans);
            tracesStore.put(traceId, currentSpans);
            // Persist timestamp indexed span ids
            long timestamp = spans.get(0).timestamp();
            Set<String> currentSpanIds = spanIdsByTsStore.get(timestamp);
            if (currentSpanIds == null) currentSpanIds = new LinkedHashSet<>();
            currentSpanIds.add(traceId);
            spanIdsByTsStore.put(timestamp, currentSpanIds);
          }
        }

        @Override public void close() {
        }
      }, TRACES_STORE_NAME, SPAN_IDS_BY_TS_STORE_NAME);
      if (traceSearchEnabled) {
        builder
            // In-memory as service names are bounded, with logging enabled to build state
            // with all values collected
            .addStateStore(Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(SERVICE_NAMES_STORE_NAME),
                Serdes.String(),
                Serdes.String()))
            // In-memory as span names are bounded, with logging enabled to build state
            // with all values collected
            .addStateStore(Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(SPAN_NAMES_STORE_NAME),
                Serdes.String(),
                namesSerde))
            // In-memory as remote-service names are bounded, with logging enabled to build state
            // with all values collected
            .addStateStore(Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(REMOTE_SERVICE_NAMES_STORE_NAME),
                Serdes.String(),
                namesSerde))
            // Persistent as values could be unbounded, but with logging enabled to build state
            // with all values collected
            .addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(AUTOCOMPLETE_TAGS_STORE_NAME),
                Serdes.String(),
                namesSerde));
        // Store service, span and remote service names
        spansStream.process(() -> new Processor<String, List<Span>>() {
              KeyValueStore<String, String> serviceNameStore;
              KeyValueStore<String, Set<String>> spanNamesStore;
              KeyValueStore<String, Set<String>> remoteServiceNamesStore;
              KeyValueStore<String, Set<String>> autocompleteTagsStore;

              @SuppressWarnings("unchecked")
              @Override public void init(ProcessorContext context) {
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
                    serviceNameStore.putIfAbsent(span.localServiceName(), span.localServiceName());
                    if (span.name() != null) { // store span names
                      Set<String> spanNames = spanNamesStore.get(span.localServiceName());
                      if (spanNames == null) spanNames = new LinkedHashSet<>();
                      if (!spanNames.contains(span.name())) {
                        spanNames.add(span.name());
                        spanNamesStore.put(span.localServiceName(), spanNames);
                      }
                    }
                    if (span.remoteServiceName() != null) { // store remote service names
                      Set<String> remoteServiceNames =
                          remoteServiceNamesStore.get(span.localServiceName());
                      if (remoteServiceNames == null) remoteServiceNames = new LinkedHashSet<>();
                      if (!remoteServiceNames.contains(span.remoteServiceName())) {
                        remoteServiceNames.add(span.remoteServiceName());
                        remoteServiceNamesStore.put(span.localServiceName(), remoteServiceNames);
                      }
                    }
                  }
                  if (!span.tags().isEmpty()) {
                    autoCompleteKeys.forEach(tagKey -> {
                      String value = span.tags().get(tagKey);
                      if (value != null) {
                        Set<String> values = autocompleteTagsStore.get(tagKey);
                        if (values == null) values = new LinkedHashSet<>();
                        if (!values.contains(value)) {
                          values.add(value);
                          autocompleteTagsStore.put(tagKey, values);
                        }
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
      }
    }
    return builder.build();
  }
}
