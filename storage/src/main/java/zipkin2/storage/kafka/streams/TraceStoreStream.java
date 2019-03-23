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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import zipkin2.Span;
import zipkin2.storage.kafka.streams.serdes.SpanSerde;
import zipkin2.storage.kafka.streams.serdes.SpansSerde;

/**
 * Aggregation and storage of Light spans into traces.
 */
public class TraceStoreStream implements Supplier<Topology> {

  // Kafka topics
  final String spansTopic;

  // Store names
  final String tracesStoreName;

  // SerDes
  final SpanSerde spanSerde;
  final SpansSerde spansSerde;

  public TraceStoreStream(String spansTopic, String tracesStoreName) {
    this.spansTopic = spansTopic;
    this.tracesStoreName = tracesStoreName;

    spanSerde = new SpanSerde();
    spansSerde = new SpansSerde();
  }

  @Override public Topology get() {
    // Preparing state stores
    StoreBuilder<KeyValueStore<String, List<Span>>> globalTracesStoreBuilder =
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(tracesStoreName),
            Serdes.String(),
            spansSerde)
            .withCachingEnabled()
            .withLoggingDisabled();

    StreamsBuilder builder = new StreamsBuilder();

    // Aggregate TraceId:Spans
    // This store could be removed once an RPC is used to find Traces per instance based on prior
    // aggregation.
    builder
        .addGlobalStore(
            globalTracesStoreBuilder,
            spansTopic,
            Consumed.with(Serdes.String(), spanSerde),
            () -> new Processor<String, Span>() {
              KeyValueStore<String, List<Span>> tracesStore;

              @Override public void init(ProcessorContext context) {
                tracesStore =
                    (KeyValueStore<String, List<Span>>) context.getStateStore(tracesStoreName);
              }

              @Override public void process(String traceId, Span span) {
                if (span == null) {
                  tracesStore.delete(traceId);
                } else {
                  List<Span> currentSpans = tracesStore.get(traceId);
                  if (currentSpans == null) {
                    List<Span> spans = new ArrayList<>();
                    spans.add(span);
                    tracesStore.put(traceId, spans);
                  } else {
                    currentSpans.add(span);
                    tracesStore.put(traceId, currentSpans);
                  }
                }
              }

              @Override public void close() {
              }
            }
        );

    return builder.build();
  }
}
