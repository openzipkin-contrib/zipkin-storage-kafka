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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.ValueMapper;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.internal.DependencyLinker;
import zipkin2.internal.Trace;
import zipkin2.storage.kafka.streams.serdes.DependencyLinkSerde;
import zipkin2.storage.kafka.streams.serdes.SpansSerde;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;
import static zipkin2.storage.kafka.streams.serdes.DependencyLinkSerde.linkKey;

/**
 * Processing of spans partitioned by trace Id, into traces and dependency links.
 */
public class AggregationTopologySupplier implements Supplier<Topology> {
  // Kafka topics
  final String spansTopicName;
  final String traceTopicName;
  final String dependencyTopicName;
  // Config
  final Duration tracesInactivityGap;
  // SerDes
  final SpansSerde spansSerde;
  final DependencyLinkSerde dependencyLinkSerde;

  public AggregationTopologySupplier(
      String spansTopicName,
      String traceTopicName,
      String dependencyTopicName,
      Duration tracesInactivityGap) {
    this.spansTopicName = spansTopicName;
    this.traceTopicName = traceTopicName;
    this.dependencyTopicName = dependencyTopicName;
    this.tracesInactivityGap = tracesInactivityGap;
    spansSerde = new SpansSerde();
    dependencyLinkSerde = new DependencyLinkSerde();
  }

  @Override public Topology get() {
    StreamsBuilder builder = new StreamsBuilder();
    // Aggregate Spans to Traces
    KStream<String, List<Span>> tracesStream =
        builder.stream(spansTopicName, Consumed.with(Serdes.String(), spansSerde))
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
    tracesStream.to(traceTopicName, Produced.with(Serdes.String(), spansSerde));
    // Map to dependency links
    tracesStream.flatMapValues(spansToDependencyLinks())
        .selectKey((key, value) -> linkKey(value))
        .to(dependencyTopicName, Produced.with(Serdes.String(), dependencyLinkSerde));
    return builder.build();
  }

  Merger<String, List<Span>> joinAggregates() {
    return (aggKey, aggOne, aggTwo) -> {
      aggOne.addAll(aggTwo);
      return Trace.merge(aggOne);
    };
  }

  Aggregator<String, List<Span>, List<Span>> aggregateSpans() {
    return (traceId, spans, allSpans) -> {
      allSpans.addAll(spans);
      return Trace.merge(allSpans);
    };
  }

  ValueMapper<List<Span>, List<DependencyLink>> spansToDependencyLinks() {
    return (spans) -> {
      if (spans == null) return new ArrayList<>();
      DependencyLinker linker = new DependencyLinker();
      return linker.putTrace(spans).link();
    };
  }
}
