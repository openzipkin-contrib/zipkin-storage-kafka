package zipkin2.storage.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import zipkin2.Span;
import zipkin2.codec.DependencyLinkBytesEncoder;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.internal.DependencyLinker;
import zipkin2.storage.kafka.internal.DependencyLinkSerde;
import zipkin2.storage.kafka.internal.SpanSerde;
import zipkin2.storage.kafka.internal.SpansSerde;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class TopologySupplier implements Supplier<Topology> {
    static final String DEPENDENCY_PAIR_PATTERN = "%s|%s";

    final String traceStoreName;
    final String serviceStoreName;
    final String dependencyStoreName;

    //    final SpanSerde spanSerde = new SpanSerde();
    final SpansSerde spansSerde = new SpansSerde();
    final DependencyLinkSerde dependencyLinkSerde = new DependencyLinkSerde();

    final SpanBytesDecoder spanBytesDecoder;
    final SpanBytesEncoder spanBytesEncoder;
    final DependencyLinkBytesEncoder dependencyLinkBytesEncoder;

    TopologySupplier(String traceStoreName, String serviceStoreName, String dependencyStoreName) {
        this.traceStoreName = traceStoreName;
        this.serviceStoreName = serviceStoreName;
        this.dependencyStoreName = dependencyStoreName;
        spanBytesDecoder = SpanBytesDecoder.PROTO3;
        spanBytesEncoder = SpanBytesEncoder.PROTO3;
        dependencyLinkBytesEncoder = DependencyLinkBytesEncoder.JSON_V1;
    }

    @Override
    public Topology get() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, List<Span>> inputStream = builder.stream(
                KafkaSpanConsumer.TOPIC,
                Consumed.<String, byte[]>with(Topology.AutoOffsetReset.EARLIEST)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.ByteArray()))
                .mapValues((ValueMapper<byte[], List<Span>>) spanBytesDecoder::decodeList);

        KStream<String, Span> spanStream = inputStream
//                .peek((key, value) -> System.out.printf("%s=%s%n", key, value))
                .flatMapValues((key, spans) -> spans)
//                .peek((key, value) -> System.out.printf("%s=%s%n", key, value))
                .map((key, span) -> KeyValue.pair(span.traceId(), span));

        KTable<String, List<Span>> aggregatedSpans = spanStream.groupByKey(Grouped.with(Serdes.String(), new SpanSerde()))
                .aggregate(ArrayList::new,
                        (key, value, aggregate) -> {
                            aggregate.add(value);
                            return aggregate;
                        }, Materialized.with(Serdes.String(), spansSerde));

        aggregatedSpans.mapValues(spanBytesEncoder::encodeList, Materialized.as(traceStoreName));

        spanStream.map((traceId, span) -> KeyValue.pair(span.localServiceName(), span.name()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .aggregate(() -> "", (serviceName, spanName, spanNames) -> spanNames.concat(",").concat(spanName))
                .mapValues((serviceName, spanNames) -> spanNames.getBytes(),
                        Materialized.as(serviceStoreName));

        aggregatedSpans.toStream()
                .filterNot((traceId, spans) -> spans.isEmpty())
                .mapValues(spans -> new DependencyLinker().putTrace(spans).link())
                .flatMapValues(dependencyLinks -> dependencyLinks)
                .groupBy((traceId, dependencyLink) -> String.format(
                        DEPENDENCY_PAIR_PATTERN, dependencyLink.parent(),
                        dependencyLink.child()),
                        Grouped.with(Serdes.String(), dependencyLinkSerde))
                .reduce((l, r) -> r)
                .mapValues(dependencyLinkBytesEncoder::encode, Materialized.as(dependencyStoreName));

        return builder.build();
    }
}
