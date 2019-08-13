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
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.internal.DependencyLinker;
import zipkin2.internal.Trace;
import zipkin2.storage.kafka.streams.serdes.DependencyLinkSerde;
import zipkin2.storage.kafka.streams.serdes.SpanSerde;
import zipkin2.storage.kafka.streams.serdes.SpansSerde;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;
import static zipkin2.storage.kafka.streams.serdes.DependencyLinkSerde.key;

/**
 *
 */
public class TraceAggregationSupplier implements Supplier<Topology> {
  // Kafka topics
  final String spansTopicName;
  final String tracesTopicName;
  final String dependencyLinksTopicName;
  // SerDes
  final SpanSerde spanSerde;
  final SpansSerde spansSerde;
  // Config
  final Duration tracesInactivityGap;
  final DependencyLinkSerde dependencyLinkSerde;

  public TraceAggregationSupplier(
      String spansTopicName,
      String tracesTopicName,
      String dependencyLinksTopicName,
      Duration tracesInactivityGap) {
    this.spansTopicName = spansTopicName;
    this.tracesTopicName = tracesTopicName;
    this.dependencyLinksTopicName = dependencyLinksTopicName;
    this.tracesInactivityGap = tracesInactivityGap;
    spanSerde = new SpanSerde();
    spansSerde = new SpansSerde();
    dependencyLinkSerde = new DependencyLinkSerde();
  }

  @Override public Topology get() {
    StreamsBuilder builder = new StreamsBuilder();
    // Aggregate Spans to Traces
    KStream<String, List<Span>> tracesStream =
        builder.stream(spansTopicName, Consumed.with(Serdes.String(), spanSerde))
            .groupByKey()
            // how long to wait for another span
            .windowedBy(SessionWindows.with(tracesInactivityGap).grace(Duration.ZERO))
            .aggregate(ArrayList::new, aggregateSpans(), joinAggregates(),
                Materialized.with(Serdes.String(), spansSerde))
            // hold until a new record tells that a window is closed and we can process it further
            .suppress(untilWindowCloses(unbounded()))
            .toStream()
            .selectKey((windowed, spans) -> windowed.key());
    // Downstream to traces topic
    tracesStream.to(tracesTopicName, Produced.with(Serdes.String(), spansSerde));
    // Map to dependency links
    tracesStream
        .flatMap(spansToDependencyLinks())
        .selectKey((key, value) -> key(value))
        .to(dependencyLinksTopicName, Produced.with(Serdes.String(), dependencyLinkSerde));
    return builder.build();
  }

  Merger<String, List<Span>> joinAggregates() {
    return (aggKey, aggOne, aggTwo) -> {
      aggOne.addAll(aggTwo);
      return Trace.merge(aggOne);
    };
  }

  Aggregator<String, Span, List<Span>> aggregateSpans() {
    return (traceId, span, spans) -> {
      if (!spans.contains(span)) spans.add(span);
      return Trace.merge(spans);
    };
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
}
