package zipkin2.storage.kafka.streams;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import zipkin2.Span;
import zipkin2.storage.kafka.streams.serdes.SpanSerde;
import zipkin2.storage.kafka.streams.serdes.SpansSerde;

public class TraceAggregationStream implements Supplier<Topology> {

  final String traceSpansTopic;
  final String tracesStoreName;
  final String tracesTopic;

  final SpanSerde spanSerde;
  final SpansSerde spansSerde;

  public TraceAggregationStream(
      String traceSpansTopic,
      String tracesStoreName,
      String tracesTopic) {
    this.traceSpansTopic = traceSpansTopic;
    this.tracesStoreName = tracesStoreName;
    this.tracesTopic = tracesTopic;

    // initialize SerDes
    spanSerde = new SpanSerde();
    spansSerde = new SpansSerde();
  }

  @Override public Topology get() {
    StreamsBuilder builder = new StreamsBuilder();

    // Aggregate Spans to Traces
    builder.stream(traceSpansTopic, Consumed.with(Serdes.String(), spanSerde))
        .peek((key, value) -> System.out.println("RECORD ===> " + key + ":" + value))
        .groupByKey()
        .aggregate(ArrayList::new, (traceId, span, spans) -> {
              spans.add(span);
              return spans;
            },
            Materialized.<String, List<Span>, KeyValueStore<Bytes, byte[]>>as(tracesStoreName)
                .withKeySerde(Serdes.String())
                .withValueSerde(spansSerde)
                .withCachingEnabled()
                .withLoggingDisabled())
        .toStream()
        .to(tracesTopic, Produced.with(Serdes.String(), spansSerde));
    return builder.build();
  }
}
