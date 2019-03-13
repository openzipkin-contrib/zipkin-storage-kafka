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
import zipkin2.DependencyLink;
import zipkin2.storage.kafka.streams.serdes.DependencyLinkSerde;
import zipkin2.storage.kafka.streams.serdes.SpansSerde;

/**
 * Stream topology supplier for Dependency aggregation.
 *
 * Source: Traces topic (aggregated Traces aggregation)
 * Store: Dependencies store (global state store)
 */
public class DependencyStoreStream implements Supplier<Topology> {
  static final String DEPENDENCY_PAIR_PATTERN = "%s|%s";

  // Kafka Topics
  final String dependenciesTopic;

  // Store names
  final String globalDependenciesStoreName;

  // SerDes
  final SpansSerde spansSerde;
  final DependencyLinkSerde dependencyLinkSerde;

  public DependencyStoreStream(String dependenciesTopic, String globalDependenciesStoreName) {
    this.dependenciesTopic = dependenciesTopic;
    this.globalDependenciesStoreName = globalDependenciesStoreName;

    spansSerde = new SpansSerde();
    dependencyLinkSerde = new DependencyLinkSerde();
  }

  @Override public Topology get() {
    // Preparing state stores
    StoreBuilder<KeyValueStore<String, DependencyLink>> globalDependenciesStoreBuilder =
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(globalDependenciesStoreName),
            Serdes.String(),
            dependencyLinkSerde)
            .withCachingEnabled()
            .withLoggingDisabled();

    StreamsBuilder builder = new StreamsBuilder();

    // Aggregate Traces to Dependencies
    builder
        .addGlobalStore(
            globalDependenciesStoreBuilder,
            dependenciesTopic,
            Consumed.with(Serdes.String(), spansSerde),
            () -> new Processor<String, DependencyLink>() {
              KeyValueStore<String, DependencyLink> dependenciesStore;

              @Override public void init(ProcessorContext context) {
                dependenciesStore =
                    (KeyValueStore<String, DependencyLink>) context.getStateStore(
                        globalDependenciesStoreName);
              }

              @Override
              public void process(String windowTraceIdLinkPair, DependencyLink dependencyLink) {
                dependenciesStore.put(windowTraceIdLinkPair, dependencyLink);
              }

              @Override public void close() { // Nothing to close
              }
            }
        );

    return builder.build();
  }
}
