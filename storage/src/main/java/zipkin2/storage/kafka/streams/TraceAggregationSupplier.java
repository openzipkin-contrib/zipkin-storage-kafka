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
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.state.SessionStore;
import zipkin2.Span;
import zipkin2.storage.kafka.streams.serdes.SpanSerde;
import zipkin2.storage.kafka.streams.serdes.SpansSerde;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;

/**
 *
 */
public class TraceReductionStream implements Supplier<Topology> {
  // Kafka topics
  final String spansTopicName;
  final String tracesTopicName;
  // SerDes
  final SpanSerde spanSerde;
  final SpansSerde spansSerde;
  // Config
  final Duration traceInactivityGap;

  public TraceReductionStream(
      String spansTopicName,
      String tracesTopicName,
      Duration traceInactivityGap) {
    this.spansTopicName = spansTopicName;
    this.tracesTopicName = tracesTopicName;
    this.traceInactivityGap = traceInactivityGap;
    spanSerde = new SpanSerde();
    spansSerde = new SpansSerde();
  }

  @Override public Topology get() {
    StreamsBuilder builder = new StreamsBuilder();
    // Aggregate Spans to Traces
    builder.stream(spansTopicName, Consumed.with(Serdes.String(), spanSerde))
        .groupByKey()
        .windowedBy(SessionWindows.with(traceInactivityGap).grace(traceInactivityGap))
        .aggregate(ArrayList::new, aggregateSpans(), joinAggregates(),
            Materialized.
                <String, List<Span>, SessionStore<Bytes, byte[]>>with(Serdes.String(), spansSerde)
                .withCachingDisabled()
                .withLoggingDisabled())
        .suppress(untilWindowCloses(unbounded()).withName("traces-suppressed"))
        .toStream()
        .selectKey((windowed, spans) -> windowed.key())
        .to(tracesTopicName, Produced.with(Serdes.String(), spansSerde));
    return builder.build();
  }

  Merger<String, List<Span>> joinAggregates() {
    return (aggKey, aggOne, aggTwo) -> {
      aggOne.addAll(aggTwo);
      return aggOne;
    };
  }

  Aggregator<String, Span, List<Span>> aggregateSpans() {
    return (traceId, span, spans) -> {
      spans.add(span);
      return spans;
    };
  }
}
