package zipkin2.storage.kafka.streams;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.storage.kafka.streams.serdes.DependencyLinkSerde;
import zipkin2.storage.kafka.streams.serdes.SpanNamesSerde;
import zipkin2.storage.kafka.streams.serdes.SpanSerde;
import zipkin2.storage.kafka.streams.serdes.SpansSerde;

public class ServiceStoreStream implements Supplier<Topology> {

  final String serviceSpanNamesTopic;

  final String globalServicesStoreName;

  final SpanNamesSerde spanNamesSerde;

  public ServiceStoreStream(
      String serviceSpanNamesTopic,
      String globalServicesStoreName) {
    this.serviceSpanNamesTopic = serviceSpanNamesTopic;
    this.globalServicesStoreName = globalServicesStoreName;

    // Initialize SerDes
    spanNamesSerde = new SpanNamesSerde();
  }

  @Override public Topology get() {
    // Preparing state stores
    StoreBuilder<KeyValueStore<String, Set<String>>> globalServiceStoreBuilder =
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(globalServicesStoreName),
            Serdes.String(),
            spanNamesSerde)
            .withCachingEnabled()
            .withLoggingDisabled();

    StreamsBuilder builder = new StreamsBuilder();

    // Aggregate Service:SpanNames
    builder
        .addGlobalStore(
            globalServiceStoreBuilder,
            serviceSpanNamesTopic,
            Consumed.with(Serdes.String(), Serdes.String()),
            () -> new Processor<String, String>() {
              KeyValueStore<String, Set<String>> servicesStore;

              @Override public void init(ProcessorContext context) {
                servicesStore =
                    (KeyValueStore<String, Set<String>>) context.getStateStore(
                        globalServicesStoreName);
              }

              @Override public void process(String serviceName, String spanName) {
                Set<String> currentSpanNames = servicesStore.get(serviceName);
                if (currentSpanNames == null) {
                  final HashSet<String> spanNames = new HashSet<>();
                  spanNames.add(spanName);
                  servicesStore.put(serviceName, spanNames);
                } else {
                  currentSpanNames.add(spanName);
                  servicesStore.put(serviceName, currentSpanNames);
                }
              }

              @Override public void close() {
              }
            });

    return builder.build();
  }
}
