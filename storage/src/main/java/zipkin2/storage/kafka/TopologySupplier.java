/*
 * Copyright 2019 [name of copyright owner]
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
package zipkin2.storage.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.codec.DependencyLinkBytesEncoder;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.internal.DependencyLinker;
import zipkin2.storage.kafka.internal.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public class TopologySupplier implements Supplier<Topology> {
    static final String DEPENDENCY_PAIR_PATTERN = "%s|%s";

    final String traceStoreName;
    final String serviceStoreName;
    final String dependencyStoreName;

    final SpansSerde spansSerde = new SpansSerde();
    final DependencyLinkSerde dependencyLinkSerde = new DependencyLinkSerde();

    final SpanBytesDecoder spanBytesDecoder;
    final SpanBytesEncoder spanBytesEncoder;
    final DependencyLinkBytesEncoder dependencyLinkBytesEncoder;
    final SpanNamesSerde spanNamesSerde;

    TopologySupplier(String traceStoreName,
                     String serviceStoreName,
                     String dependencyStoreName) {
        this.traceStoreName = traceStoreName;
        this.serviceStoreName = serviceStoreName;
        this.dependencyStoreName = dependencyStoreName;

        spanBytesDecoder = SpanBytesDecoder.PROTO3;
        spanBytesEncoder = SpanBytesEncoder.PROTO3;
        dependencyLinkBytesEncoder = DependencyLinkBytesEncoder.JSON_V1;
        spanNamesSerde = new SpanNamesSerde();
    }

    @Override
    public Topology get() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Span> spanStream = builder.stream(
                KafkaSpanConsumer.TOPIC,
                Consumed.<String, byte[]>with(Topology.AutoOffsetReset.EARLIEST)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.ByteArray()))
                .mapValues(spanBytesDecoder::decodeOne);

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

        aggregatedSpans.to(traceStoreName, Produced.valueSerde(spansSerde));

        builder.globalTable(traceStoreName,
                Materialized
                        .<String, List<Span>, KeyValueStore<Bytes, byte[]>>as(traceStoreName)
                        .withValueSerde(spansSerde));

        spanStream.map((traceId, span) -> KeyValue.pair(span.localServiceName(), span.name()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .aggregate(HashSet::new,
                        (serviceName, spanName, spanNames) -> {
                            spanNames.add(spanName);
                            return spanNames;
                        },
                        Materialized
                                .<String, Set<String>, KeyValueStore<Bytes, byte[]>>with(Serdes.String(), spanNamesSerde)
                                .withLoggingDisabled().withCachingDisabled())
                .toStream().to(serviceStoreName, Produced.with(Serdes.String(), spanNamesSerde));

        builder.globalTable(
                serviceStoreName,
                Materialized.<String, Set<String>, KeyValueStore<Bytes, byte[]>>as(serviceStoreName)
                        .withValueSerde(spanNamesSerde));

        aggregatedSpans
                .filterNot((traceId, spans) -> spans.isEmpty())
                .mapValues(spans -> new DependencyLinker().putTrace(spans).link())
                .flatMapValues(dependencyLinks -> dependencyLinks)
                .groupBy((traceId, dependencyLink) -> String.format(
                        DEPENDENCY_PAIR_PATTERN, dependencyLink.parent(),
                        dependencyLink.child()),
                        Grouped.with(Serdes.String(), dependencyLinkSerde))
                .reduce((l, r) -> r,
                        Materialized.<String, DependencyLink, KeyValueStore<Bytes, byte[]>>with(Serdes.String(), dependencyLinkSerde)
                                .withLoggingDisabled().withCachingDisabled())
                .toStream().to(dependencyStoreName, Produced.valueSerde(dependencyLinkSerde));

        builder.globalTable(dependencyStoreName,
                Materialized
                        .<String, DependencyLink, KeyValueStore<Bytes, byte[]>>as(dependencyStoreName)
                        .withValueSerde(dependencyLinkSerde));

        return builder.build();
    }
}
