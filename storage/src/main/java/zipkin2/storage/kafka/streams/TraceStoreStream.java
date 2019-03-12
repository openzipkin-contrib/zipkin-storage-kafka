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
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import zipkin2.Span;
import zipkin2.storage.kafka.streams.serdes.SpanSerde;
import zipkin2.storage.kafka.streams.serdes.SpansSerde;

public class TraceStoreStream implements Supplier<Topology> {

  final String spansTopic;
  final String traceSpansTopic;
  final String tracesTopic;

  final String tracesStoreName;
  final String globalTracesStoreName;

  final SpanSerde spanSerde;
  final SpansSerde spansSerde;

  public TraceStoreStream(
      String spansTopic,
      String traceSpansTopic,
      String tracesTopic,
      String tracesStoreName,
      String globalTracesStoreName) {
    this.spansTopic = spansTopic;
    this.traceSpansTopic = traceSpansTopic;
    this.tracesTopic = tracesTopic;
    this.tracesStoreName = tracesStoreName;
    this.globalTracesStoreName = globalTracesStoreName;

    // Initialize SerDes
    spanSerde = new SpanSerde();
    spansSerde = new SpansSerde();
  }

  @Override public Topology get() {
    // Preparing state stores
    StoreBuilder<KeyValueStore<String, List<Span>>> globalTracesStoreBuilder =
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(globalTracesStoreName),
            Serdes.String(),
            spansSerde)
            .withCachingEnabled()
            .withLoggingDisabled();

    StreamsBuilder builder = new StreamsBuilder();

    // Aggregate Spans to Traces
    builder.stream(traceSpansTopic, Consumed.with(Serdes.String(), spanSerde))
        .groupByKey()
        .aggregate(ArrayList::new, (s, span, spans) -> {
              spans.add(span);
              return spans;
            },
            Materialized.<String, List<Span>, KeyValueStore<Bytes, byte[]>>as(tracesStoreName)
                .withKeySerde(Serdes.String())
                .withValueSerde(spansSerde)
                .withCachingEnabled()
                .withLoggingDisabled())
        .toStream()
        .to(tracesTopic);

    // Aggregate TraceId:Spans
    // This store could be removed once an RPC is used to find Traces per instance based on prior
    // aggregation.
    builder
        .addGlobalStore(
            globalTracesStoreBuilder,
            traceSpansTopic,
            Consumed.with(Serdes.String(), spanSerde),
            () -> new Processor<String, Span>() {
              KeyValueStore<String, List<Span>> tracesStore;

              @Override public void init(ProcessorContext context) {
                tracesStore =
                    (KeyValueStore<String, List<Span>>) context.getStateStore(
                        globalTracesStoreName);
              }

              @Override public void process(String traceId, Span span) {
                List<Span> currentSpans = tracesStore.get(traceId);
                if (currentSpans == null) {
                  List<Span> spans = new ArrayList<>();
                  spans.add(span);
                  tracesStore.put(traceId, spans);
                } else {
                  currentSpans.add(span);
                  tracesStore.put(traceId, currentSpans);
                }
              }

              @Override public void close() {
              }
            }
        );

    return builder.build();
  }
}
