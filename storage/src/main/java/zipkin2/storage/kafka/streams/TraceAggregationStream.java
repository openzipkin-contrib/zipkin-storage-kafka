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
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.state.SessionStore;
import zipkin2.Span;
import zipkin2.internal.DependencyLinker;
import zipkin2.storage.kafka.streams.serdes.DependencyLinkSerde;
import zipkin2.storage.kafka.streams.serdes.SpanSerde;
import zipkin2.storage.kafka.streams.serdes.SpansSerde;

import static java.util.stream.Collectors.toList;

/**
 * Aggregation of Spans partitioned by TraceId into a Trace ChangeLog
 */
public class TraceAggregationStream implements Supplier<Topology> {
  static final String LINK_PATTERN = "%s|%s";

  // Kafka topics
  final String traceSpansTopic;
  final String tracesTopic;
  final String dependenciesTopic;

  // Store names
  final String tracesStoreName;

  // SerDes
  final SpanSerde spanSerde;
  final SpansSerde spansSerde;
  final DependencyLinkSerde dependencyLinkSerde;

  final Duration traceInactivityGap;

  final DependencyLinker dependencyLinker;

  public TraceAggregationStream(
      String traceSpansTopic,
      String tracesStoreName,
      String tracesTopic,
      String dependenciesTopic,
      Duration traceInactivityGap) {
    this.traceSpansTopic = traceSpansTopic;
    this.tracesStoreName = tracesStoreName;
    this.tracesTopic = tracesTopic;
    this.dependenciesTopic = dependenciesTopic;
    this.traceInactivityGap = traceInactivityGap;

    spanSerde = new SpanSerde();
    spansSerde = new SpansSerde();
    dependencyLinkSerde = new DependencyLinkSerde();
    dependencyLinker = new DependencyLinker();
  }

  @Override public Topology get() {
    StreamsBuilder builder = new StreamsBuilder();

    // Aggregate Spans to Traces
    final KStream<String, List<Span>> traceStream =
        builder.stream(traceSpansTopic, Consumed.with(Serdes.String(), spanSerde))
            .groupByKey()
            .windowedBy(SessionWindows.with(traceInactivityGap))
            .aggregate(ArrayList::new,
                (traceId, span, spans) -> {
                  if (span == null) { // Cleaning state
                    return null;
                  } else {
                    if (spans == null) {
                      return Collections.singletonList(span);
                    } else {
                      spans.add(span);
                      return spans;
                    }
                  }
                },
                (traceId, spans1, spans2) -> {
                  spans2.addAll(spans1);
                  return spans2;
                },
                Materialized.<String, List<Span>, SessionStore<Bytes, byte[]>>as(tracesStoreName)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(spansSerde)
                    .withCachingDisabled()
                    .withLoggingDisabled())
            .toStream()
            .map((windowed, spans) -> KeyValue.pair(
                String.format("%s:%s", windowed.window().start(), windowed.key()), spans))
            .through(tracesTopic, Produced.with(Serdes.String(), spansSerde));

    traceStream
        .flatMap((windowTraceId, spans) ->
            dependencyLinker.putTrace(spans).link()
                .stream()
                .map(dependencyLink -> {
                  String linkPair =
                      String.format(LINK_PATTERN, dependencyLink.parent(), dependencyLink.child());
                  return KeyValue.pair(linkPair, dependencyLink);
                }).collect(toList()))
        .to(dependenciesTopic, Produced.with(Serdes.String(), dependencyLinkSerde));

    return builder.build();
  }
}
