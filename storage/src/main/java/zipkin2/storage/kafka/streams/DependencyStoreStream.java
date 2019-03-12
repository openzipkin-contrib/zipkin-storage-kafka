package zipkin2.storage.kafka.streams;

import java.util.List;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.internal.DependencyLinker;
import zipkin2.storage.kafka.streams.serdes.DependencyLinkSerde;
import zipkin2.storage.kafka.streams.serdes.SpansSerde;

public class DependencyStoreStream implements Supplier<Topology> {
  static final String DEPENDENCY_PAIR_PATTERN = "%s|%s";

  final String tracesTopic;

  final String globalDependenciesStoreName;

  final SpansSerde spansSerde;
  final DependencyLinkSerde dependencyLinkSerde;

  public DependencyStoreStream(
      String tracesTopic,
      String globalDependenciesStoreName) {
    this.tracesTopic = tracesTopic;
    this.globalDependenciesStoreName = globalDependenciesStoreName;

    // Initialize SerDes
    spansSerde = new SpansSerde();
    dependencyLinkSerde = new DependencyLinkSerde();
  }

  @Override public Topology get() {
    // Preparing state stores
    StoreBuilder<KeyValueStore<String, DependencyLink>> globalDependenciesStoreBuilder =
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(globalDependenciesStoreName),
            Serdes.String(),
            dependencyLinkSerde)
            .withCachingEnabled()
            .withLoggingDisabled();

    StreamsBuilder builder = new StreamsBuilder();

    // Aggregate Traces to Dependencies
    builder
        .addGlobalStore(
            globalDependenciesStoreBuilder,
            tracesTopic,
            Consumed.with(Serdes.String(), spansSerde),
            () -> new Processor<String, List<Span>>() {
              KeyValueStore<String, DependencyLink> dependenciesStore;

              @Override public void init(ProcessorContext context) {
                dependenciesStore =
                    (KeyValueStore<String, DependencyLink>) context.getStateStore(
                        globalDependenciesStoreName);
              }

              @Override public void process(String traceId, List<Span> spans) {
                List<DependencyLink> dependencyLinks =
                    new DependencyLinker().putTrace(spans).link();
                for (DependencyLink dependencyLink : dependencyLinks) {
                  String dependencyKey =
                      String.format(
                          DEPENDENCY_PAIR_PATTERN, dependencyLink.parent(),
                          dependencyLink.child());
                  DependencyLink currentDependencyLink = dependenciesStore.get(dependencyKey);
                  if (currentDependencyLink == null) {
                    dependenciesStore.put(dependencyKey, dependencyLink);
                  } else {
                    DependencyLink aggDependencyLink =
                        DependencyLink.newBuilder()
                            .parent(currentDependencyLink.parent())
                            .child(currentDependencyLink.child())
                            .callCount(currentDependencyLink.callCount() + dependencyLink.callCount())
                            .errorCount(currentDependencyLink.errorCount() + dependencyLink.errorCount())
                            .build();
                    dependenciesStore.put(dependencyKey, aggDependencyLink);
                  }
                }
              }

              @Override public void close() {
              }
            }
        );

    return builder.build();
  }
}
