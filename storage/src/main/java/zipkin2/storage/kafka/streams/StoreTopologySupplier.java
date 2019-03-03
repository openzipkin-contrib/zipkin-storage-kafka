package zipkin2.storage.kafka.streams;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.storage.kafka.streams.serdes.DependencyLinkSerde;
import zipkin2.storage.kafka.streams.serdes.SpanNamesSerde;
import zipkin2.storage.kafka.streams.serdes.SpansSerde;

public class StoreTopologySupplier implements Supplier<Topology> {

  final String traceStoreName;
  final String serviceStoreName;
  final String dependencyStoreName;


  final SpansSerde spansSerde;
  final DependencyLinkSerde dependencyLinkSerde;
  final SpanNamesSerde spanNamesSerde;

  public StoreTopologySupplier(String traceStoreName, String serviceStoreName,
      String dependencyStoreName) {
    this.traceStoreName = traceStoreName;
    this.serviceStoreName = serviceStoreName;
    this.dependencyStoreName = dependencyStoreName;

    spansSerde = new SpansSerde();
    dependencyLinkSerde = new DependencyLinkSerde();
    spanNamesSerde = new SpanNamesSerde();
  }

  @Override public Topology get() {
    // Preparing state stores
    StoreBuilder<KeyValueStore<String, byte[]>> traceStoreBuilder = Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(traceStoreName),
        Serdes.String(),
        Serdes.ByteArray());
    traceStoreBuilder.build();
    StoreBuilder<KeyValueStore<String, byte[]>> serviceStoreBuilder = Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(serviceStoreName),
        Serdes.String(),
        Serdes.ByteArray());
    serviceStoreBuilder.build();
    StoreBuilder<KeyValueStore<String, byte[]>> dependencyStoreBuilder =
        Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(dependencyStoreName),
            Serdes.String(),
            Serdes.ByteArray());
    dependencyStoreBuilder.build();

    StreamsBuilder builder = new StreamsBuilder();

    // Prepare local state from traces
    builder.globalTable(traceStoreName,
        Materialized
            .<String, List<Span>, KeyValueStore<Bytes, byte[]>>as(traceStoreName)
            .withValueSerde(spansSerde));


    // Prepare local state from service names
    builder.globalTable(
        serviceStoreName,
        Materialized.<String, Set<String>, KeyValueStore<Bytes, byte[]>>as(serviceStoreName)
            .withValueSerde(spanNamesSerde));

    // Preparing local state from dependencies
    builder.globalTable(dependencyStoreName,
        Materialized
            .<String, DependencyLink, KeyValueStore<Bytes, byte[]>>as(dependencyStoreName)
            .withValueSerde(dependencyLinkSerde));

    return builder.build();
  }
}
