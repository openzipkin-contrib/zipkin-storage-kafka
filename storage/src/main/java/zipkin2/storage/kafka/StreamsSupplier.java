package zipkin2.storage.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import zipkin2.Span;
import zipkin2.codec.DependencyLinkBytesEncoder;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.internal.DependencyLinker;
import zipkin2.storage.kafka.internal.DependencyLinkSerde;
import zipkin2.storage.kafka.internal.SpansSerde;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;

public class StreamsSupplier implements Supplier<KafkaStreams> {
    private static final String DEPENDENCY_PAIR_PATTERN = "%s|%s";

    final Properties streamsConfig;
    final KeyValueBytesStoreSupplier traceStoreSupplier;
    final KeyValueBytesStoreSupplier serviceSpanStoreSupplier;
    final KeyValueBytesStoreSupplier dependencyStoreSupplier;

    //    final SpanSerde spanSerde = new SpanSerde();
    final SpansSerde spansSerde = new SpansSerde();
    final DependencyLinkSerde dependencyLinkSerde = new DependencyLinkSerde();

    public StreamsSupplier(Properties streamsConfig, KeyValueBytesStoreSupplier traceStoreSupplier, KeyValueBytesStoreSupplier serviceSpanStoreSupplier, KeyValueBytesStoreSupplier dependencyStoreSupplier) {
        this.streamsConfig = streamsConfig;
        this.traceStoreSupplier = traceStoreSupplier;
        this.serviceSpanStoreSupplier = serviceSpanStoreSupplier;
        this.dependencyStoreSupplier = dependencyStoreSupplier;
    }

    @Override
    public KafkaStreams get() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, List<Span>> inputStream = builder.stream(
                KafkaSpanConsumer.TOPIC,
                Consumed.<String, List<Span>>with(Topology.AutoOffsetReset.EARLIEST)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(spansSerde));

        KStream<String, Span> spanStream = inputStream
                .flatMapValues((key, spans) -> spans)
                .map((key, span) -> KeyValue.pair(span.traceId(), span));

        KTable<String, List<Span>> aggregatedSpans = spanStream.groupByKey()
                .aggregate(ArrayList::new,
                        (key, value, aggregate) -> {
                            aggregate.add(value);
                            return aggregate;
                        }, Materialized.with(Serdes.String(), spansSerde));

        aggregatedSpans.mapValues((traceId, spans) -> SpanBytesEncoder.PROTO3.encodeList(spans),
                Materialized.as(traceStoreSupplier));

        spanStream.map((traceId, span) -> KeyValue.pair(span.localServiceName(), span.name()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .aggregate(() -> "", (serviceName, spanName, spanNames) -> spanNames.concat(",").concat(spanName))
                .mapValues((serviceName, spanNames) -> spanNames.getBytes(),
                        Materialized.as(serviceSpanStoreSupplier));

        aggregatedSpans.toStream()
                .filterNot((traceId, spans) -> spans.isEmpty())
                .mapValues(spans -> new DependencyLinker().putTrace(spans).link())
                .flatMapValues(dependencyLinks -> dependencyLinks)
                .groupBy((traceId, dependencyLink) -> String.format(
                        DEPENDENCY_PAIR_PATTERN, dependencyLink.parent(),
                        dependencyLink.child()),
                        Grouped.with(Serdes.String(), dependencyLinkSerde))
                .reduce((l, r) -> r)
                .mapValues(DependencyLinkBytesEncoder.JSON_V1::encode, Materialized.as(dependencyStoreSupplier));

        return new KafkaStreams(builder.build(), streamsConfig);
    }
}
