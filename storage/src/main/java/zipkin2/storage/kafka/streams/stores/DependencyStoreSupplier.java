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
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import zipkin2.DependencyLink;
import zipkin2.storage.kafka.streams.serdes.DependencyLinkSerde;
import zipkin2.storage.kafka.streams.serdes.DependencyLinksSerde;
import zipkin2.storage.kafka.streams.serdes.NamesSerde;

/**
 * Stream topology supplier for Dependency aggregation.
 * <p>
 * Source: Traces topic (aggregated Traces aggregation) Store: Dependencies store (global state
 * store)
 */
public class DependencyStoreSupplier implements Supplier<Topology> {
  public static final String DEPENDENCY_LINKS_STORE_NAME = "zipkin_dependency_links";

  // Kafka Topics
  final String dependencyLinksTopic;
  // Limits
  final Duration retentionPeriod;
  final Duration windowSize;
  // SerDes
  final DependencyLinkSerde dependencyLinkSerde;
  final DependencyLinksSerde dependencyLinksSerde;
  final NamesSerde namesSerde;

  public DependencyStoreSupplier(String dependencyLinksTopic,
      Duration retentionPeriod,
      Duration windowSize) {
    this.dependencyLinksTopic = dependencyLinksTopic;
    this.retentionPeriod = retentionPeriod;
    this.windowSize = windowSize;

    dependencyLinkSerde = new DependencyLinkSerde();
    dependencyLinksSerde = new DependencyLinksSerde();
    namesSerde = new NamesSerde();
  }

  @Override public Topology get() {
    StreamsBuilder builder = new StreamsBuilder();

    builder
        // Add state stores
        .addStateStore(Stores.windowStoreBuilder(
            Stores.persistentWindowStore(
                DEPENDENCY_LINKS_STORE_NAME,
                retentionPeriod,
                windowSize,
                false),
            Serdes.String(),
            dependencyLinkSerde
        ))
        // Consume dependency links stream
        .stream(dependencyLinksTopic, Consumed.with(Serdes.String(), dependencyLinkSerde))
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
            Instant instant = Instant.ofEpochMilli(context.timestamp());
            WindowStoreIterator<DependencyLink> currentLinkWindow =
                dependencyLinksStore.fetch(linkKey, instant.minus(windowSize), instant);
            // Get latest window. Only two are possible.
            KeyValue<Long, DependencyLink> windowAndValue = null;
            if (currentLinkWindow.hasNext()) windowAndValue = currentLinkWindow.next();
            if (currentLinkWindow.hasNext()) windowAndValue = currentLinkWindow.next();

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
