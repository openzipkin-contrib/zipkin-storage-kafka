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
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.state.KeyValueStore;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.internal.DependencyLinker;
import zipkin2.storage.kafka.streams.serdes.DependencyLinkSerde;
import zipkin2.storage.kafka.streams.serdes.SpanSerde;
import zipkin2.storage.kafka.streams.serdes.SpansSerde;

/**
 * Reduction of span dependency events, with call/error counter equals to 0 or 1, into ever
 * increasing dependency link with updated counters.
 */
public class DependencyAggregationStream implements Supplier<Topology> {
  static final String KEY_PATTERN = "%s:%s";
  // Kafka topics
  final String tracesTopicName;
  final String spanDependenciesTopicName;
  final String dependenciesTopicName;
  // SerDes
  final SpanSerde spanSerde;
  final SpansSerde spansSerde;
  final DependencyLinkSerde dependencyLinkSerde;

  public DependencyAggregationStream(
      String tracesTopicName,
      String spanDependenciesTopicName,
      String dependenciesTopicName) {
    this.tracesTopicName = tracesTopicName;
    this.spanDependenciesTopicName = spanDependenciesTopicName;
    this.dependenciesTopicName = dependenciesTopicName;
    spanSerde = new SpanSerde();
    spansSerde = new SpansSerde();
    dependencyLinkSerde = new DependencyLinkSerde();
  }

  @Override public Topology get() {
    StreamsBuilder builder = new StreamsBuilder();
    // Changelog of dependency links over time
    builder.stream(tracesTopicName, Consumed.with(Serdes.String(), spansSerde))
        .flatMap(spansToDependencyLinks())
        .through(spanDependenciesTopicName, Produced.with(Serdes.String(), dependencyLinkSerde))
        .groupByKey()
        .reduce(reduceDependencyLinks(),
            Materialized.<String, DependencyLink, KeyValueStore<Bytes, byte[]>>with(
                Serdes.String(),
                dependencyLinkSerde).withLoggingDisabled().withCachingEnabled())
        .toStream()
        .selectKey((key, value) -> key)
        .to(dependenciesTopicName, Produced.with(Serdes.String(), dependencyLinkSerde));
    return builder.build();
  }

  KeyValueMapper<String, List<Span>, List<KeyValue<String, DependencyLink>>> spansToDependencyLinks() {
    return (windowed, spans) -> {
      if (spans == null) return new ArrayList<>();
      DependencyLinker linker = new DependencyLinker();
      return linker.putTrace(spans).link().stream()
          .map(link -> KeyValue.pair(key(link), link))
          .collect(Collectors.toList());
    };
  }

  /**
   * Reducing link events into links with updated results
   */
  Reducer<DependencyLink> reduceDependencyLinks() {
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

  String key(DependencyLink link) {
    return String.format(KEY_PATTERN, link.parent(), link.child());
  }
}
