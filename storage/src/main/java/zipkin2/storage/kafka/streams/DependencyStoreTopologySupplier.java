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

/**
 * Windowed storage of dependency links.
 */
public final class DependencyStoreTopologySupplier implements Supplier<Topology> {
  public static final String DEPENDENCIES_STORE_NAME = "zipkin-dependencies";

  // Kafka topics
  final String dependencyTopic;
  // Configs
  final Duration dependencyTtl;
  final Duration dependencyWindowSize;
  // Flags
  final boolean dependencyQueryEnabled;
  // SerDes
  final DependencyLinkSerde dependencyLinkSerde;

  public DependencyStoreTopologySupplier(
      String dependencyTopic,
      Duration dependencyTtl,
      Duration dependencyWindowSize,
      boolean dependencyQueryEnabled) {
    this.dependencyTopic = dependencyTopic;
    this.dependencyTtl = dependencyTtl;
    this.dependencyWindowSize = dependencyWindowSize;
    this.dependencyQueryEnabled = dependencyQueryEnabled;
    dependencyLinkSerde = new DependencyLinkSerde();
  }

  @Override public Topology get() {
    StreamsBuilder builder = new StreamsBuilder();
    if (dependencyQueryEnabled) {
      // Dependency links window store
      builder.addStateStore(
          // Disabling logging to avoid long starting times
          Stores.windowStoreBuilder(
              Stores.persistentWindowStore(
                  DEPENDENCIES_STORE_NAME,
                  dependencyTtl,
                  dependencyWindowSize,
                  false),
              Serdes.String(),
              dependencyLinkSerde
          ).withLoggingDisabled());
      // Consume dependency links stream
      builder.stream(dependencyTopic, Consumed.with(Serdes.String(), dependencyLinkSerde))
          // Storage
          .process(() -> new Processor<String, DependencyLink>() {
            ProcessorContext context;
            WindowStore<String, DependencyLink> dependenciesStore;

            @SuppressWarnings("unchecked")
            @Override public void init(ProcessorContext context) {
              this.context = context;
              dependenciesStore =
                  (WindowStore<String, DependencyLink>) context.getStateStore(
                      DEPENDENCIES_STORE_NAME);
            }

            @Override public void process(String linkKey, DependencyLink link) {
              // Event time
              Instant now = Instant.ofEpochMilli(context.timestamp());
              Instant from = now.minus(dependencyWindowSize);
              WindowStoreIterator<DependencyLink> currentLinkWindow =
                  dependenciesStore.fetch(linkKey, from, now);
              // Get latest window. Only two are possible.
              KeyValue<Long, DependencyLink> windowAndValue = null;
              if (currentLinkWindow.hasNext()) windowAndValue = currentLinkWindow.next();
              if (currentLinkWindow.hasNext()) windowAndValue = currentLinkWindow.next();
              // Persist dependency link per window
              if (windowAndValue != null) {
                DependencyLink currentLink = windowAndValue.value;
                DependencyLink aggregated = currentLink.toBuilder()
                    .callCount(currentLink.callCount() + link.callCount())
                    .errorCount(currentLink.errorCount() + link.errorCount())
                    .build();
                dependenciesStore.put(linkKey, aggregated, windowAndValue.key);
              } else {
                dependenciesStore.put(linkKey, link);
              }
            }

            @Override public void close() {
            }
          }, DEPENDENCIES_STORE_NAME);
    }
    return builder.build();
  }
}
