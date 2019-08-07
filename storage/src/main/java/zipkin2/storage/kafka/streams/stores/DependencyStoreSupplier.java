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
import java.util.List;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.DependencyLink;
import zipkin2.storage.kafka.streams.serdes.DependencyLinkSerde;
import zipkin2.storage.kafka.streams.serdes.DependencyLinksSerde;

/**
 * Stream topology supplier for Dependency aggregation.
 * <p>
 * Source: Traces topic (aggregated Traces aggregation) Store: Dependencies store (global state
 * store)
 */
public class DependencyStoreSupplier implements Supplier<Topology> {
  public static final String DEPENDENCY_LINKS_BY_TIMESTAMP_STORE_NAME =
      "zipkin_dependency_link_ids_by_timestamp";

  static final Logger LOG = LoggerFactory.getLogger(DependencyStoreSupplier.class);
  static final String DEPENDENCY_LINKS_STORE_NAME = "zipkin_dependency_links";

  // Kafka Topics
  final String dependencyLinksTopic;
  // Limits
  final Duration scanFrequency;
  final Duration maxAge;
  // SerDes
  final DependencyLinkSerde dependencyLinkSerde;
  final DependencyLinksSerde dependencyLinksSerde;

  public DependencyStoreSupplier(String dependencyLinksTopic,
      Duration scanFrequency,
      Duration maxAge) {
    this.dependencyLinksTopic = dependencyLinksTopic;
    this.scanFrequency = scanFrequency;
    this.maxAge = maxAge;

    dependencyLinkSerde = new DependencyLinkSerde();
    dependencyLinksSerde = new DependencyLinksSerde();
  }

  @Override public Topology get() {
    StreamsBuilder builder = new StreamsBuilder();

    builder
        // Add state stores
        .addStateStore(Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(DEPENDENCY_LINKS_STORE_NAME),
            Serdes.String(),
            dependencyLinkSerde
        ))
        .addStateStore(Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(DEPENDENCY_LINKS_BY_TIMESTAMP_STORE_NAME),
            Serdes.Long(),
            dependencyLinksSerde
        ))
        // Consume dependency links stream
        .stream(dependencyLinksTopic, Consumed.with(Serdes.String(), dependencyLinkSerde))
        // Storage
        .process(() -> new Processor<String, DependencyLink>() {
          ProcessorContext context;
          KeyValueStore<String, DependencyLink> dependencyLinksStore;
          KeyValueStore<Long, List<DependencyLink>> timestampAndDependencyLinkIdsStore;

          @Override
          public void init(ProcessorContext context) {
            this.context = context;

            dependencyLinksStore =
                (KeyValueStore<String, DependencyLink>) context.getStateStore(
                    DEPENDENCY_LINKS_STORE_NAME);
            timestampAndDependencyLinkIdsStore =
                (KeyValueStore<Long, List<DependencyLink>>) context.getStateStore(
                    DEPENDENCY_LINKS_BY_TIMESTAMP_STORE_NAME);

            context.schedule(
                scanFrequency,
                PunctuationType.STREAM_TIME,
                timestamp -> {
                  // TODO check this logic
                  final long cutoff = timestamp - maxAge.toMillis();
                  final long ttl = cutoff * 1000;
                  final long now = System.currentTimeMillis() * 1000;

                  // Scan all records indexed
                  try (
                      final KeyValueIterator<Long, List<DependencyLink>> all = timestampAndDependencyLinkIdsStore
                          .range(ttl, now)) {
                    int deletions = 0;
                    while (all.hasNext()) {
                      final KeyValue<Long, List<DependencyLink>> record = all.next();
                      timestampAndDependencyLinkIdsStore.delete(record.key);
                    }
                    LOG.info("Traces deletion emitted: {}, older than {}",
                        deletions, Instant.ofEpochMilli(cutoff));
                  }
                });
          }

          @Override
          public void process(String dependencyLinkKey, DependencyLink link) {
            DependencyLink currentLink = dependencyLinksStore.get(dependencyLinkKey);
            if (currentLink == null) {
              currentLink = link;
            } else {
              currentLink = DependencyLink.newBuilder()
                  .callCount(currentLink.callCount() + link.callCount())
                  .errorCount(currentLink.errorCount() + link.errorCount())
                  .child(currentLink.child())
                  .parent(currentLink.parent())
                  .build();
            }
            dependencyLinksStore.put(dependencyLinkKey, currentLink);
            long timestamp = context.timestamp();
            List<DependencyLink> currentLinks = timestampAndDependencyLinkIdsStore.get(
                timestamp);
            if (currentLinks == null) {
              currentLinks = new ArrayList<>();
            }
            currentLinks.add(currentLink);
            timestampAndDependencyLinkIdsStore.put(timestamp, currentLinks);
          }

          @Override
          public void close() {
          }
        }, DEPENDENCY_LINKS_STORE_NAME, DEPENDENCY_LINKS_BY_TIMESTAMP_STORE_NAME);

    return builder.build();
  }
}
