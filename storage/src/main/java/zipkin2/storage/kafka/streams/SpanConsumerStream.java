package zipkin2.storage.kafka.streams;

import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import zipkin2.Span;
import zipkin2.storage.kafka.streams.serdes.SpanNamesSerde;
import zipkin2.storage.kafka.streams.serdes.SpanSerde;

public class SpanConsumerStream implements Supplier<Topology> {
  final String spansTopic;
  final String serviceSpanNamesTopic;
  final String traceSpansTopic;

  final SpanSerde spanSerde;

  public SpanConsumerStream(
      String spansTopic,
      String serviceSpanNamesTopic,
      String traceSpansTopic) {
    this.spansTopic = spansTopic;
    this.serviceSpanNamesTopic = serviceSpanNamesTopic;
    this.traceSpansTopic = traceSpansTopic;

    // Initialize SerDes
    spanSerde = new SpanSerde();
  }

  @Override public Topology get() {
    StreamsBuilder builder = new StreamsBuilder();

    // Raw Spans Stream
    KStream<String, Span> spanStream =
        builder.stream(spansTopic, Consumed.with(Serdes.String(), spanSerde));

    // Repartition of Light Spans by Trace Id
    spanStream.map((s, span) -> {
      zipkin2.Span.Builder spanBuilder = zipkin2.Span.newBuilder()
          .traceId(span.traceId())
          .parentId(span.parentId())
          .id(span.id())
          .kind(span.kind())
          .shared(span.shared())
          .name(span.name())
          .timestamp(span.timestamp())
          .duration(span.duration())
          .localEndpoint(span.localEndpoint())
          .remoteEndpoint(span.remoteEndpoint());
      return KeyValue.pair(span.traceId(), spanBuilder.build());
    }).to(traceSpansTopic, Produced.with(Serdes.String(), spanSerde));

    // Stream of serviceName:spanName
    spanStream
        .map((traceId, span) -> KeyValue.pair(span.localServiceName(), span.name()))
        .to(serviceSpanNamesTopic, Produced.with(Serdes.String(), Serdes.String()));

    return builder.build();
  }
}
