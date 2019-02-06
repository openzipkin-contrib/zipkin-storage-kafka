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
import zipkin2.storage.kafka.internal.DependencyLinkSerde;
import zipkin2.storage.kafka.internal.SpanNamesSerde;
import zipkin2.storage.kafka.internal.SpanSerde;
import zipkin2.storage.kafka.internal.SpansSerde;

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

    TopologySupplier(String traceStoreName, String serviceStoreName, String dependencyStoreName) {
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

        KTable<String, List<Span>> aggregatedSpans = spanStream.groupByKey(
                Grouped.with(Serdes.String(), new SpanSerde()))
                .aggregate(ArrayList::new,
                        (key, value, aggregate) -> {
                            aggregate.add(value);
                            return aggregate;
                        },
                        Materialized.<String, List<Span>, KeyValueStore<Bytes, byte[]>>as(traceStoreName)
                                .withKeySerde(Serdes.String()).withValueSerde(spansSerde));

        spanStream.map((traceId, span) -> KeyValue.pair(span.localServiceName(), span.name()))
                .peek((key, value) -> System.out.println(key + "=" + value))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .aggregate(HashSet::new,
                        (serviceName, spanName, spanNames) -> {
                            spanNames.add(spanName);
                            return spanNames;
                        },
                        Materialized.<String, Set<String>, KeyValueStore<Bytes, byte[]>>as(serviceStoreName)
                                .withKeySerde(Serdes.String()).withValueSerde(spanNamesSerde));

        aggregatedSpans.toStream()
                .filterNot((traceId, spans) -> spans.isEmpty())
                .mapValues(spans -> new DependencyLinker().putTrace(spans).link())
                .flatMapValues(dependencyLinks -> dependencyLinks)
                .groupBy((traceId, dependencyLink) -> String.format(
                        DEPENDENCY_PAIR_PATTERN, dependencyLink.parent(),
                        dependencyLink.child()),
                        Grouped.with(Serdes.String(), dependencyLinkSerde))
                .reduce((l, r) -> r,
                        Materialized.<String, DependencyLink, KeyValueStore<Bytes, byte[]>>as(dependencyStoreName)
                                .withKeySerde(Serdes.String()).withValueSerde(dependencyLinkSerde));

        return builder.build();
    }
}
