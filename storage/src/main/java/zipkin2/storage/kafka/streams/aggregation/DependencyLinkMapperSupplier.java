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
package zipkin2.storage.kafka.streams.aggregation;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
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
public class DependencyLinkMapperSupplier implements Supplier<Topology> {
  static final String KEY_PATTERN = "%s:%s";
  // Kafka topics
  final String tracesTopicName;
  final String dependencyLinksTopicName;
  // SerDes
  final SpanSerde spanSerde;
  final SpansSerde spansSerde;
  final DependencyLinkSerde dependencyLinkSerde;

  public DependencyLinkMapperSupplier(
      String tracesTopicName,
      String dependencyLinksTopicName) {
    this.tracesTopicName = tracesTopicName;
    this.dependencyLinksTopicName = dependencyLinksTopicName;
    spanSerde = new SpanSerde();
    spansSerde = new SpansSerde();
    dependencyLinkSerde = new DependencyLinkSerde();
  }

  @Override public Topology get() {
    StreamsBuilder builder = new StreamsBuilder();
    // Changelog of dependency links over time
    builder.stream(tracesTopicName, Consumed.with(Serdes.String(), spansSerde))
        .flatMap(spansToDependencyLinks())
        .selectKey((key, value) -> key(value))
        .to(dependencyLinksTopicName, Produced.with(Serdes.String(), dependencyLinkSerde));
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

  String key(DependencyLink link) {
    return String.format(KEY_PATTERN, link.parent(), link.child());
  }
}
