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
import java.util.List;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.Span;
import zipkin2.storage.kafka.streams.serdes.SpansSerde;

public class RetentionTopologySupplier implements Supplier<Topology> {
  static final Logger LOG = LoggerFactory.getLogger(RetentionTopologySupplier.class);

  final String tracesTopic;
  final String traceTsStoreName;

  final Duration scanFrequency;
  final Duration maxAge;

  final SpansSerde spansSerde;

  public RetentionTopologySupplier(String tracesTopic, Duration scanFrequency,
      Duration maxAge) {
    this.tracesTopic = tracesTopic;
    this.scanFrequency = scanFrequency;
    this.maxAge = maxAge;
    traceTsStoreName = tracesTopic + "-ts";
    spansSerde = new SpansSerde();
  }

  @Override public Topology get() {
    StreamsBuilder builder = new StreamsBuilder();
    builder
        .addStateStore(Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(traceTsStoreName),
            Serdes.String(),
            Serdes.Long()))
        .stream(tracesTopic, Consumed.with(Serdes.String(), spansSerde))
        .transform(
            () -> new Transformer<String, List<Span>, KeyValue<String, List<Span>>>() {
              private KeyValueStore<String, Long> stateStore;

              @Override public void init(ProcessorContext context) {
                this.stateStore =
                    (KeyValueStore<String, Long>) context.getStateStore(traceTsStoreName);
                context.schedule(
                    scanFrequency,
                    PunctuationType.WALL_CLOCK_TIME,
                    timestamp -> {
                      final long cutoff = timestamp - maxAge.toMillis();
                      final long ttl = Long.valueOf(cutoff + "000");

                      // scan over all the keys in this partition's store
                      // this can be optimized, but just keeping it simple.
                      // this might take a while, so the Streams timeouts should take this into account
                      try (final KeyValueIterator<String, Long> all = stateStore.all()) {
                        int deletions = 0;
                        while (all.hasNext()) {
                          final KeyValue<String, Long> record = all.next();
                          if (record.value != null && record.value < ttl) {
                            deletions++;
                            // if a record's last update was older than our cutoff, emit a tombstone.
                            context.forward(record.key, null);
                          }
                        }
                        LOG.info("Traces deletion emitted: {}, older than {}", deletions,
                            Instant.ofEpochMilli(cutoff));
                      }
                    }
                );
              }

              @Override
              public KeyValue<String, List<Span>> transform(String key, List<Span> value) {
                if (value == null) {
                  stateStore.delete(key);
                } else {
                  if (value.size() > 1) {
                    Long timestamp = value.get(0).timestamp();
                    stateStore.put(key, timestamp);
                  }
                }
                return null;
              }

              @Override public void close() {
              }
            }, traceTsStoreName)
        .to(tracesTopic);
    return builder.build();
  }
}
