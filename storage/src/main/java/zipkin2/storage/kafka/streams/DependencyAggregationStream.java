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
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.DependencyLink;
import zipkin2.storage.kafka.streams.serdes.DependencyLinkSerde;

/**
 * Reduction of span dependency events, with call/error counter equals to 0 or 1, into ever
 * increasing dependency link with updated counters.
 */
public class DependencyAggregationStream implements Supplier<Topology> {
  static final Logger LOG = LoggerFactory.getLogger(DependencyAggregationStream.class);
  // Kafka topics
  final String spanDependencyTopic;
  final String dependenciesTopic;
  // Store names
  final String dependenciesStoreName;
  // SerDes
  final DependencyLinkSerde dependencyLinkSerde;

  public DependencyAggregationStream(
      String spanDependencyTopic,
      String dependenciesTopic,
      String dependenciesStoreName) {
    this.spanDependencyTopic = spanDependencyTopic;
    this.dependenciesTopic = dependenciesTopic;
    this.dependenciesStoreName = dependenciesStoreName;
    dependencyLinkSerde = new DependencyLinkSerde();
  }

  @Override public Topology get() {
    StreamsBuilder builder = new StreamsBuilder();
    // Aggregate Spans to Traces
    builder.stream(spanDependencyTopic, Consumed.with(Serdes.String(), dependencyLinkSerde))
        .groupByKey()
        .reduce(reduceDependencyLink())
        .toStream()
        // Changelog of dependency links over time
        .peek((key, value) -> LOG.debug("Dependency transition: {}:{}", key, value))
        .to(dependenciesTopic, Produced.with(Serdes.String(), dependencyLinkSerde));
    return builder.build();
  }

  /**
   * Reducing link events into links with updated results
   */
  Reducer<DependencyLink> reduceDependencyLink() {
    return (link1, link2) -> {
      if (link2 == null) {
        return link1;
      } else {
        return DependencyLink.newBuilder()
            .parent(link1.parent())
            .child(link1.child())
            .callCount(link1.callCount() + link2.callCount())
            .errorCount(link1.errorCount() + link2.errorCount())
            .build();
      }
    };
  }
}
