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
import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.Span;
import zipkin2.storage.kafka.streams.serdes.SpanIdsSerde;
import zipkin2.storage.kafka.streams.serdes.SpansSerde;

/**
 * Aggregation and storage of spans into traces.
 */
public class TraceStoreSupplier implements Supplier<Topology> {
  static final Logger LOG = LoggerFactory.getLogger(TraceStoreSupplier.class);

  // Kafka topics
  final String tracesTopic;
  // Store names
  final String tracesStoreName;
  final String timestampAndTraceIdsStoreName;
  // Limits
  final Duration scanFrequency;
  final Duration maxAge;
  // SerDes
  //final SpanSerde spanSerde;
  final SpansSerde spansSerde;
  final SpanIdsSerde spanIdsSerde;
  // Index Service
  //final SpanIndexService spanIndexService;

  public TraceStoreSupplier(
      String tracesTopic,
      String tracesStoreName, String timestampAndTraceIdsStoreName, Duration scanFrequency,
      Duration maxAge) {
    this.tracesTopic = tracesTopic;
    this.tracesStoreName = tracesStoreName;
    this.timestampAndTraceIdsStoreName = timestampAndTraceIdsStoreName;
    this.scanFrequency = scanFrequency;
    this.maxAge = maxAge;
    //this.spanIndexService = spanIndexService;
    //spanSerde = new SpanSerde();
    spansSerde = new SpansSerde();
    spanIdsSerde = new SpanIdsSerde();
  }

  @Override public Topology get() {
    // Preparing state stores
    //StoreBuilder<KeyValueStore<String, List<Span>>> globalTracesStoreBuilder =
    //    Stores.keyValueStoreBuilder(
    //        Stores.persistentKeyValueStore(tracesStoreName),
    //        Serdes.String(),
    //        spansSerde)
    //        .withCachingEnabled()
    //        .withLoggingDisabled();

    StreamsBuilder builder = new StreamsBuilder();

    builder.addStateStore(Stores.keyValueStoreBuilder(
       Stores.persistentKeyValueStore(tracesStoreName),
       Serdes.String(),
       spansSerde
    ));

    builder.addStateStore(Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(timestampAndTraceIdsStoreName),
        Serdes.Long(),
        spanIdsSerde
    ));

    // Aggregate TraceId:Spans
    // This store could be removed once an RPC is used to getTraceIds Traces per instance based on prior
    // aggregation.
    //builder
    //    .addGlobalStore(
    //        globalTracesStoreBuilder,
    //        tracesTopic,
    //        Consumed.with(Serdes.String(), spanSerde),
    //        () -> new Processor<String, Span>() {
    //          KeyValueStore<String, List<Span>> tracesStore;
    //
    //          @Override public void init(ProcessorContext context) {
    //            tracesStore =
    //                (KeyValueStore<String, List<Span>>) context.getStateStore(tracesStoreName);
    //          }
    //
    //          @Override public void process(String traceId, Span span) {
    //            if (span == null) {
    //              tracesStore.delete(traceId);
    //              spanIndexService.deleteByTraceId(traceId);
    //            } else {
    //              List<Span> currentSpans = tracesStore.get(traceId);
    //              if (currentSpans == null) {
    //                currentSpans = Collections.singletonList(span);
    //              } else {
    //                currentSpans.add(span);
    //              }
    //              tracesStore.put(traceId, currentSpans);
    //              spanIndexService.insert(span);
    //            }
    //          }
    //
    //          @Override public void close() {
    //          }
    //        }
    //    );

    KStream<String, List<Span>> traces =
        builder.stream(tracesTopic, Consumed.with(Serdes.String(), spansSerde));

    traces.process(() -> new Processor<String, List<Span>>() {
      KeyValueStore<String, List<Span>> tracesStore;
      KeyValueStore<Long, Set<String>> timestampAndSpanIdsStore;
      ProcessorContext context;

      @Override public void init(ProcessorContext context) {
        this.context = context;
        tracesStore =
            (KeyValueStore<String, List<Span>>) context.getStateStore(tracesStoreName);
        timestampAndSpanIdsStore =
            (KeyValueStore<Long, Set<String>>) context.getStateStore(timestampAndTraceIdsStoreName);
      }

      @Override public void process(String traceId, List<Span> spans) {
        //if (spans == null) {
        //  tracesStore.delete(traceId);
        //} else {
        List<Span> currentSpans = tracesStore.get(traceId);
        if (currentSpans == null) {
          currentSpans = spans;
        } else {
          currentSpans.addAll(spans);
        }
        Set<String> currentSpanIds = timestampAndSpanIdsStore.get(context.timestamp());
        if (currentSpanIds == null) {
          currentSpanIds = new HashSet<>();
        }
        currentSpanIds.add(traceId);
        tracesStore.put(traceId, currentSpans);
        timestampAndSpanIdsStore.put(context.timestamp(), currentSpanIds);
        //}
      }

      @Override public void close() {
      }
    }, tracesStoreName, timestampAndTraceIdsStoreName);

    traces.process(() -> new Processor<String, List<Span>>() {
      KeyValueStore<String, List<Span>> tracesStore;
      KeyValueStore<Long, Set<String>> traceIdsPerTimestampStore;

      @Override public void init(ProcessorContext context) {
        tracesStore =
            (KeyValueStore<String, List<Span>>) context.getStateStore(tracesStoreName);
        traceIdsPerTimestampStore =
            (KeyValueStore<Long, Set<String>>) context.getStateStore(timestampAndTraceIdsStoreName);
        context.schedule(
            scanFrequency,
            PunctuationType.STREAM_TIME,
            timestamp -> {
              final long cutoff = timestamp - maxAge.toMillis();
              final long ttl = cutoff * 1000;
              final long now = System.currentTimeMillis() * 1000;

              // Scan all records indexed
              try (final KeyValueIterator<Long, Set<String>> all = traceIdsPerTimestampStore.range(ttl, now)) {
                int deletions = 0;
                while (all.hasNext()) {
                  final KeyValue<Long, Set<String>> record = all.next();
                  traceIdsPerTimestampStore.delete(record.key);
                  for (String traceId : record.value) {
                    tracesStore.delete(traceId);
                  }
                }
                LOG.info("Traces deletion emitted: {}, older than {}",
                    deletions, Instant.ofEpochMilli(cutoff));
              }
            });
      }

      @Override public void process(String s, List<Span> spans) {
      }

      @Override public void close() {
      }
    }, tracesStoreName, timestampAndTraceIdsStoreName);

    return builder.build();
  }
}
