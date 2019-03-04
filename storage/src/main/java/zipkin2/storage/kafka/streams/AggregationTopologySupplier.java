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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.internal.DependencyLinker;
import zipkin2.storage.kafka.streams.serdes.DependencyLinkSerde;
import zipkin2.storage.kafka.streams.serdes.SpanNamesSerde;
import zipkin2.storage.kafka.streams.serdes.SpanSerde;
import zipkin2.storage.kafka.streams.serdes.SpansSerde;

/**
 * Aggregation of spans into Traces, Services and Dependencies.
 */
public class AggregationTopologySupplier implements Supplier<Topology> {
  static final String DEPENDENCY_PAIR_PATTERN = "%s|%s";

  final String spansTopic;

  final String traceStoreName;
  final String serviceStoreName;
  final String dependencyStoreName;

  final SpansSerde spansSerde;
  final DependencyLinkSerde dependencyLinkSerde;
  final SpanNamesSerde spanNamesSerde;

  public AggregationTopologySupplier(String spansTopic,
      String traceStoreName,
      String serviceStoreName,
      String dependencyStoreName) {
    this.spansTopic = spansTopic;
    this.traceStoreName = traceStoreName;
    this.serviceStoreName = serviceStoreName;
    this.dependencyStoreName = dependencyStoreName;

    spansSerde = new SpansSerde();
    dependencyLinkSerde = new DependencyLinkSerde();
    spanNamesSerde = new SpanNamesSerde();
  }

  @Override
  public Topology get() {
    StreamsBuilder builder = new StreamsBuilder();

    // Mapping spans
    KStream<String, Span> spanStream = builder.stream(
        spansTopic,
        Consumed.<String, byte[]>with(Topology.AutoOffsetReset.EARLIEST)
            .withKeySerde(Serdes.String())
            .withValueSerde(Serdes.ByteArray()))
        .mapValues(SpanBytesDecoder.PROTO3::decodeOne);

    // Aggregating Spans by Trace Id
    KStream<String, List<Span>> aggregatedSpans = spanStream.groupByKey(
        Grouped.with(Serdes.String(), new SpanSerde()))
        .aggregate(ArrayList::new,
            (key, value, aggregate) -> {
              aggregate.add(value);
              return aggregate;
            },
            Materialized
                .<String, List<Span>, KeyValueStore<Bytes, byte[]>>with(Serdes.String(), spansSerde)
                .withLoggingDisabled().withCachingDisabled())
        .toStream();

    // Downstream Aggregated Spans to Topic
    aggregatedSpans.to(traceStoreName, Produced.valueSerde(spansSerde));


    // Aggregating Spans into Service -> Spans map
    spanStream.map((traceId, span) -> KeyValue.pair(span.localServiceName(), span.name()))
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .aggregate(HashSet::new,
            (serviceName, spanName, spanNames) -> {
              spanNames.add(spanName);
              return spanNames;
            },
            Materialized
                .<String, Set<String>, KeyValueStore<Bytes, byte[]>>with(Serdes.String(),
                    spanNamesSerde)
                .withLoggingDisabled().withCachingDisabled())
        .toStream().to(serviceStoreName, Produced.with(Serdes.String(), spanNamesSerde));

    // Aggregating traces into dependencies
    aggregatedSpans
        .filterNot((traceId, spans) -> spans.isEmpty())
        .mapValues(spans -> new DependencyLinker().putTrace(spans).link())
        .flatMapValues(dependencyLinks -> dependencyLinks)
        .groupBy((traceId, dependencyLink) ->
                String.format(
                    DEPENDENCY_PAIR_PATTERN, dependencyLink.parent(),
                    dependencyLink.child()),
            Grouped.with(Serdes.String(), dependencyLinkSerde))
        .reduce((l, r) ->
                DependencyLink.newBuilder()
                    .parent(r.parent())
                    .child(r.child())
                    .callCount(r.callCount() + l.callCount())
                    .errorCount(r.errorCount() + l.errorCount())
                    .build(),
            Materialized.<String, DependencyLink, KeyValueStore<Bytes, byte[]>>with(Serdes.String(),
                dependencyLinkSerde)
                .withLoggingDisabled().withCachingDisabled())
        .toStream().to(dependencyStoreName, Produced.valueSerde(dependencyLinkSerde));

    return builder.build();
  }
}
