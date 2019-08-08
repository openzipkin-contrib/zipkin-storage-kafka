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
package zipkin2.storage.kafka.streams.stores;

import java.time.Duration;
import java.time.Instant;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.Span;
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

  static final Logger LOG = LoggerFactory.getLogger(TraceStoreSupplier.class);

  // Kafka topics
  final String tracesTopic;
  // Limits
  final Duration scanFrequency;
  final Duration maxAge;
  // SerDes
  final SpansSerde spansSerde;
  final SpanIdsSerde spanIdsSerde;
  final NamesSerde namesSerde;

  public TraceStoreSupplier(String tracesTopic, Duration scanFrequency, Duration maxAge) {
    this.tracesTopic = tracesTopic;
    this.scanFrequency = scanFrequency;
    this.maxAge = maxAge;

    spansSerde = new SpansSerde();
    spanIdsSerde = new SpanIdsSerde();
    namesSerde = new NamesSerde();
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
        ));

    KStream<String, List<Span>> stream = builder
        .stream(tracesTopic, Consumed.with(Serdes.String(), spansSerde));

    stream
        .process(() -> new Processor<String, List<Span>>() {
          KeyValueStore<String, List<Span>> tracesStore;
          KeyValueStore<Long, Set<String>> spanIdsByTsStore;
          ProcessorContext context;

          @Override public void init(ProcessorContext context) {
            this.context = context;
            tracesStore =
                (KeyValueStore<String, List<Span>>) context.getStateStore(TRACES_STORE_NAME);
            spanIdsByTsStore =
                (KeyValueStore<Long, Set<String>>) context.getStateStore(SPAN_IDS_BY_TS_STORE_NAME);

            context.schedule(
                scanFrequency,
                PunctuationType.STREAM_TIME,
                timestamp -> {
                  long cutoff = maxAge.toMillis();
                  long ttl = timestamp - cutoff;
                  long ttlMicro = ttl * 1000;
                  try (final KeyValueIterator<Long, Set<String>> all =
                           spanIdsByTsStore.range(0L, ttlMicro)) {
                    int deletions = 0;
                    while (all.hasNext()) {
                      final KeyValue<Long, Set<String>> record = all.next();
                      spanIdsByTsStore.delete(record.key);
                      for (String traceId : record.value) {
                        tracesStore.delete(traceId);
                        deletions++;
                      }
                    }
                    if (deletions > 0) {
                      LOG.info("Traces deletion emitted: {}, older than {}",
                          deletions, Instant.ofEpochMilli(ttl));
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

    stream.process(() -> new Processor<String, List<Span>>() {
      KeyValueStore<String, String> serviceNameStore;
      KeyValueStore<String, Set<String>> spanNamesStore;
      KeyValueStore<String, Set<String>> remoteServiceNamesStore;

      @Override
      public void init(ProcessorContext context) {
        serviceNameStore =
            (KeyValueStore<String, String>) context.getStateStore(SERVICE_NAMES_STORE_NAME);
        spanNamesStore =
            (KeyValueStore<String, Set<String>>) context.getStateStore(SPAN_NAMES_STORE_NAME);
        remoteServiceNamesStore =
            (KeyValueStore<String, Set<String>>) context.getStateStore(
                REMOTE_SERVICE_NAMES_STORE_NAME);
      }

      @Override
      public void process(String traceId, List<Span> spans) {
        for (Span span : spans) {
          if (span.localServiceName() != null) {
            serviceNameStore.putIfAbsent(span.localServiceName(), span.localServiceName());
            if (span.name() != null) {
              Set<String> spanNames = spanNamesStore.get(span.localServiceName());
              if (spanNames == null) spanNames = new HashSet<>();
              spanNames.add(span.name());
              spanNamesStore.put(span.localServiceName(), spanNames);
            }
            if (span.remoteServiceName() != null) {
              Set<String> remoteServiceNames = remoteServiceNamesStore.get(span.localServiceName());
              if (remoteServiceNames == null) remoteServiceNames = new HashSet<>();
              remoteServiceNames.add(span.remoteServiceName());
              remoteServiceNamesStore.put(span.localServiceName(), remoteServiceNames);
            }
          }
        }
      }

      @Override
      public void close() {

      }
    }, SERVICE_NAMES_STORE_NAME, SPAN_NAMES_STORE_NAME, REMOTE_SERVICE_NAMES_STORE_NAME);

    return builder.build();
  }
}
