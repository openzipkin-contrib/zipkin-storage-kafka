/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
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
import org.apache.kafka.streams.state.Stores;
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
public final class SpanAggregationTopology implements Supplier<Topology> {
  static final String TRACE_AGGREGATION_STORE = "trace-aggregation";
  static final Duration TRACE_AGGREGATION_RETENTION = Duration.ofDays(1);
  // Kafka topics
  final String spansTopic;
  final String traceTopic;
  final String dependencyTopic;
  // Config
  final Duration traceTimeout;
  // Flags
  final boolean aggregationEnabled;
  // SerDes
  final SpansSerde spansSerde;
  final DependencyLinkSerde dependencyLinkSerde;

  public SpanAggregationTopology(
    String spansTopic,
    String traceTopic,
    String dependencyTopic,
    Duration traceTimeout,
    boolean aggregationEnabled
  ) {
    this.spansTopic = spansTopic;
    this.traceTopic = traceTopic;
    this.dependencyTopic = dependencyTopic;
    this.traceTimeout = traceTimeout;
    this.aggregationEnabled = aggregationEnabled;
    spansSerde = new SpansSerde();
    dependencyLinkSerde = new DependencyLinkSerde();
  }

  @Override public Topology get() {
    StreamsBuilder builder = new StreamsBuilder();
    if (aggregationEnabled) {
      // Aggregate Spans to Traces
      KStream<String, List<Span>> tracesStream =
        builder.stream(spansTopic, Consumed.with(Serdes.String(), spansSerde))
          .groupByKey()
          // how long to wait for another span
          .windowedBy(SessionWindows.with(traceTimeout).grace(Duration.ZERO))
          .aggregate(ArrayList::new, aggregateSpans(), joinAggregates(),
            Materialized
              .<String, List<Span>>as(
                Stores.persistentSessionStore(
                  TRACE_AGGREGATION_STORE,
                  TRACE_AGGREGATION_RETENTION))
              .withKeySerde(Serdes.String())
              .withValueSerde(spansSerde)
              .withLoggingDisabled()
              .withCachingEnabled())
          // hold until a new record tells that a window is closed and we can process it further
          .suppress(untilWindowCloses(unbounded()))
          .toStream()
          .selectKey((windowed, spans) -> windowed.key());
      // Downstream to traces topic
      tracesStream.to(traceTopic, Produced.with(Serdes.String(), spansSerde));
      // Map to dependency links
      tracesStream.flatMapValues(spansToDependencyLinks())
        .selectKey((key, value) -> linkKey(value))
        .to(dependencyTopic, Produced.with(Serdes.String(), dependencyLinkSerde));
    }
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
