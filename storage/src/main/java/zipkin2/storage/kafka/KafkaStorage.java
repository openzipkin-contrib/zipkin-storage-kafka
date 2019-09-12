/*
 * Copyright 2019 jeqo
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

import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.Call;
import zipkin2.CheckResult;
import zipkin2.storage.AutocompleteTags;
import zipkin2.storage.ServiceAndSpanNames;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StorageComponent;
import zipkin2.storage.kafka.internal.NoopServiceAndSpanNames;
import zipkin2.storage.kafka.internal.NoopSpanStore;
import zipkin2.storage.kafka.streams.AggregationTopologySupplier;
import zipkin2.storage.kafka.streams.DependencyStoreTopologySupplier;
import zipkin2.storage.kafka.streams.TraceStoreTopologySupplier;

/**
 * Zipkin's Kafka Storage.
 * <p>
 * Storage implementation based on Kafka Streams, supporting:
 * <ul>
 *   <li>repartitioning of spans,</li>
 *   <li>trace aggregation,</li>
 *   <li>autocomplete tags, and</li>
 *   <li>indexing of traces and dependencies.</li>
 * </ul>
 */
public class KafkaStorage extends StorageComponent {
  static final Logger LOG = LoggerFactory.getLogger(KafkaStorage.class);

  // Kafka Storage modes
  final boolean spanConsumerEnabled, searchEnabled;
  // Autocomplete Tags
  final List<String> autocompleteKeys;
  // Kafka Storage configs
  final String storageDir;
  final long minTracesStored;
  final int httpPort;
  // Kafka Topics
  final String spansTopicName, traceTopicName, dependencyTopicName;
  // Kafka Clients config
  final Properties adminConfig;
  final Properties producerConfig;
  // Kafka Streams topology configs
  final Properties aggregationStreamConfig, traceStoreStreamConfig, dependencyStoreStreamConfig;
  final Topology aggregationTopology, traceStoreTopology, dependencyStoreTopology;
  final BiFunction<String, Integer, String> httpBaseUrl;
  // Resources
  volatile AdminClient adminClient;
  volatile Producer<String, byte[]> producer;
  volatile KafkaStreams traceAggregationStream, traceStoreStream, dependencyStoreStream;
  volatile Server server;
  volatile boolean closeCalled;

  KafkaStorage(Builder builder) {
    // Kafka Storage modes
    this.spanConsumerEnabled = builder.spanConsumerEnabled;
    this.searchEnabled = builder.searchEnabled;
    // Autocomplete tags
    this.autocompleteKeys = builder.autocompleteKeys;
    // Kafka Topics config
    this.spansTopicName = builder.spansTopicName;
    this.traceTopicName = builder.traceTopicName;
    this.dependencyTopicName = builder.dependencyTopicName;
    // Storage directories
    this.storageDir = builder.storageDir;
    this.minTracesStored = builder.minTracesStored;
    this.httpPort = builder.httpPort;
    this.httpBaseUrl = builder.httpBaseUrl;
    // Kafka Configs
    this.adminConfig = builder.adminConfig;
    this.producerConfig = builder.producerConfig;
    this.aggregationStreamConfig = builder.aggregationStreamConfig;
    this.traceStoreStreamConfig = builder.traceStoreStreamConfig;
    this.dependencyStoreStreamConfig = builder.dependencyStoreStreamConfig;

    aggregationTopology = new AggregationTopologySupplier(
        spansTopicName,
        traceTopicName,
        dependencyTopicName,
        builder.traceTimeout).get();
    traceStoreTopology = new TraceStoreTopologySupplier(
        spansTopicName,
        autocompleteKeys,
        builder.traceTtl,
        builder.traceTtlCheckInterval,
        builder.minTracesStored).get();
    dependencyStoreTopology = new DependencyStoreTopologySupplier(
        dependencyTopicName,
        builder.dependencyTtl,
        builder.dependencyWindowSize).get();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public SpanConsumer spanConsumer() {
    checkResources();
    if (spanConsumerEnabled) {
      return new KafkaSpanConsumer(this);
    } else { // NoopSpanConsumer
      return list -> Call.create(null);
    }
  }

  @Override
  public ServiceAndSpanNames serviceAndSpanNames() {
    checkResources();
    if (searchEnabled) {
      return new KafkaSpanStore(this);
    } else {
      return new NoopServiceAndSpanNames();
    }
  }

  @Override
  public SpanStore spanStore() {
    checkResources();
    if (searchEnabled) {
      return new KafkaSpanStore(this);
    } else {
      return new NoopSpanStore();
    }
  }

  @Override public AutocompleteTags autocompleteTags() {
    checkResources();
    if (searchEnabled) {
      return new KafkaAutocompleteTags(this);
    } else {
      return super.autocompleteTags();
    }
  }

  void checkResources() {
    if (spanConsumerEnabled) {
      getAggregationStream();
    }
    if (searchEnabled) {
      getTraceStoreStream();
      getDependencyStoreStream();
      getServer();
    }
  }

  @Override public CheckResult check() {
    try {
      KafkaFuture<String> maybeClusterId = getAdminClient().describeCluster().clusterId();
      maybeClusterId.get(1, TimeUnit.SECONDS);
      if (spanConsumerEnabled) {
        KafkaStreams.State state = getAggregationStream().state();
        if (!state.isRunning()) {
          return CheckResult.failed(
              new IllegalStateException("Aggregation stream not running. " + state));
        }
      }
      if (searchEnabled) {
        KafkaStreams.State stateTraceStore = getTraceStoreStream().state();
        if (!stateTraceStore.isRunning()) {
          return CheckResult.failed(
              new IllegalStateException("Store stream not running. " + stateTraceStore));
        }
        KafkaStreams.State stateDependencyStore = getDependencyStoreStream().state();
        if (!stateDependencyStore.isRunning()) {
          return CheckResult.failed(
              new IllegalStateException("Store stream not running. " + stateDependencyStore));
        }
        if (!getServer().activePort().isPresent()) {
          return CheckResult.failed(
              new IllegalStateException(
                  "Storage HTTP server not running. " + stateDependencyStore));
        }
      }
      return CheckResult.OK;
    } catch (Exception e) {
      return CheckResult.failed(e);
    }
  }

  @Override public void close() {
    if (closeCalled) return;
    synchronized (this) {
      if (!closeCalled) {
        doClose();
        closeCalled = true;
      }
    }
  }

  void doClose() {
    try {
      if (adminClient != null) adminClient.close(Duration.ofSeconds(1));
      if (producer != null) {
        producer.close(Duration.ofSeconds(1));
      }
      if (traceStoreStream != null) {
        traceStoreStream.close(Duration.ofSeconds(1));
      }
      if (dependencyStoreStream != null) {
        dependencyStoreStream.close(Duration.ofSeconds(1));
      }
      if (traceAggregationStream != null) {
        traceAggregationStream.close(Duration.ofSeconds(1));
      }
      if (server != null) server.close();
    } catch (Exception | Error e) {
      LOG.warn("error closing client {}", e.getMessage(), e);
    }
  }

  Producer<String, byte[]> getProducer() {
    if (producer == null) {
      synchronized (this) {
        if (producer == null) {
          producer = new KafkaProducer<>(producerConfig);
        }
      }
    }
    return producer;
  }

  AdminClient getAdminClient() {
    if (adminClient == null) {
      synchronized (this) {
        if (adminClient == null) {
          adminClient = AdminClient.create(adminConfig);
        }
      }
    }
    return adminClient;
  }

  KafkaStreams getTraceStoreStream() {
    if (traceStoreStream == null) {
      synchronized (this) {
        if (traceStoreStream == null) {
          try {
            traceStoreStream = new KafkaStreams(traceStoreTopology, traceStoreStreamConfig);
            traceStoreStream.start();
          } catch (Exception e) {
            LOG.error("Error starting trace store process", e);
            traceStoreStream = null;
          }
        }
      }
    }
    return traceStoreStream;
  }

  KafkaStreams getDependencyStoreStream() {
    if (dependencyStoreStream == null) {
      synchronized (this) {
        if (dependencyStoreStream == null) {
          try {
            dependencyStoreStream =
                new KafkaStreams(dependencyStoreTopology, dependencyStoreStreamConfig);
            dependencyStoreStream.start();
          } catch (Exception e) {
            LOG.error("Error starting dependency store", e);
            dependencyStoreStream = null;
          }
        }
      }
    }
    return dependencyStoreStream;
  }

  KafkaStreams getAggregationStream() {
    if (traceAggregationStream == null) {
      synchronized (this) {
        if (traceAggregationStream == null) {
          try {
            traceAggregationStream =
                new KafkaStreams(aggregationTopology, aggregationStreamConfig);
            traceAggregationStream.start();
          } catch (Exception e) {
            LOG.error("Error loading aggregation process", e);
            traceAggregationStream = null;
          }
        }
      }
    }
    return traceAggregationStream;
  }

  Server getServer() {
    if (server == null) {
      synchronized (this) {
        if (server == null) {
          try {
            ServerBuilder builder = new ServerBuilder();
            builder.http(httpPort);
            builder.annotatedService(new KafkaStoreHttpService(this));
            server = builder.build();
            server.start();
          } catch (Exception e) {
            LOG.error("Error starting http server", e);
            server = null;
          }
        }
      }
    }
    return server;
  }

  public static class Builder extends StorageComponent.Builder {
    boolean spanConsumerEnabled = true;
    boolean searchEnabled = true;

    List<String> autocompleteKeys = new ArrayList<>();

    Duration traceTtl = Duration.ofDays(3);
    Duration traceTtlCheckInterval = Duration.ofHours(1);
    Duration traceTimeout = Duration.ofMinutes(1);
    Duration dependencyTtl = Duration.ofDays(7);
    Duration dependencyWindowSize = Duration.ofMinutes(1);

    long minTracesStored = 10_000;
    int httpPort = 9412;
    BiFunction<String, Integer, String> httpBaseUrl =
        (hostname, port) -> "http://" + hostname + ":" + port;

    String storageDir = "/tmp/zipkin-storage-kafka";

    Properties adminConfig = new Properties();
    Properties producerConfig = new Properties();
    Properties aggregationStreamConfig = new Properties();
    Properties traceStoreStreamConfig = new Properties();
    Properties dependencyStoreStreamConfig = new Properties();

    String traceStoreStreamAppId = "zipkin-trace-store";
    String dependencyStoreStreamAppId = "zipkin-dependency-store";
    String aggregationStreamAppId = "zipkin-aggregation";

    String spansTopicName = "zipkin-spans";
    String traceTopicName = "zipkin-trace";
    String dependencyTopicName = "zipkin-dependency";

    Builder() {
      // Kafka Producer configuration
      producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
      producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
      producerConfig.put(ProducerConfig.BATCH_SIZE_CONFIG, 500_000);
      producerConfig.put(ProducerConfig.LINGER_MS_CONFIG, 5);
      // Trace Aggregation Stream Topology configuration
      aggregationStreamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
          Serdes.StringSerde.class);
      aggregationStreamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
          Serdes.ByteArraySerde.class);
      aggregationStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, aggregationStreamAppId);
      aggregationStreamConfig.put(StreamsConfig.STATE_DIR_CONFIG, traceStoreDirectory());
      aggregationStreamConfig.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
      // Trace Store Stream Topology configuration
      traceStoreStreamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
          Serdes.StringSerde.class);
      traceStoreStreamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
          Serdes.ByteArraySerde.class);
      traceStoreStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, traceStoreStreamAppId);
      traceStoreStreamConfig.put(StreamsConfig.STATE_DIR_CONFIG, traceStoreDirectory());
      traceStoreStreamConfig.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
      traceStoreStreamConfig.put(StreamsConfig.APPLICATION_SERVER_CONFIG, hostInfo());
      // Dependency Store Stream Topology configuration
      dependencyStoreStreamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
          Serdes.StringSerde.class);
      dependencyStoreStreamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
          Serdes.ByteArraySerde.class);
      dependencyStoreStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,
          dependencyStoreStreamAppId);
      dependencyStoreStreamConfig.put(StreamsConfig.STATE_DIR_CONFIG, dependencyStoreDirectory());
      dependencyStoreStreamConfig.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
      dependencyStoreStreamConfig.put(StreamsConfig.APPLICATION_SERVER_CONFIG, hostInfo());
    }

    String hostInfo() {
      String hostInfo = "localhost";
      try {
        hostInfo = InetAddress.getLocalHost().getHostName() + ":" + httpPort;
      } catch (UnknownHostException e) {
        e.printStackTrace();
      }
      return hostInfo;
    }

    @Override
    public Builder strictTraceId(boolean strictTraceId) {
      if (!strictTraceId) throw new IllegalArgumentException("non-strict trace ID not supported");
      return this;
    }

    @Override
    public Builder searchEnabled(boolean searchEnabled) {
      this.searchEnabled = searchEnabled;
      return this;
    }

    @Override
    public Builder autocompleteKeys(List<String> keys) {
      if (keys == null) throw new NullPointerException("keys == null");
      this.autocompleteKeys = keys;
      return this;
    }

    /**
     * Enable consuming spans from collectors, aggregation, and store them in Kafka topics.
     * <p>
     * When disabled, a NoopSpanConsumer is instantiated to do nothing with incoming spans.
     */
    public Builder spanConsumerEnabled(boolean spanConsumerEnabled) {
      this.spanConsumerEnabled = spanConsumerEnabled;
      return this;
    }

    public Builder httpPort(int httpPort) {
      this.httpPort = httpPort;
      traceStoreStreamConfig.put(StreamsConfig.APPLICATION_SERVER_CONFIG, hostInfo());
      dependencyStoreStreamConfig.put(StreamsConfig.APPLICATION_SERVER_CONFIG, hostInfo());
      return this;
    }

    /**
     * How long to wait for a span in order to trigger a trace as completed.
     */
    public Builder traceTimeout(Duration traceTimeout) {
      if (traceTimeout == null) {
        throw new NullPointerException("traceTimeout == null");
      }
      this.traceTimeout = traceTimeout;
      return this;
    }

    /**
     * Kafka Bootstrap Servers list to establish connection with a Cluster.
     */
    public Builder bootstrapServers(String bootstrapServers) {
      if (bootstrapServers == null) throw new NullPointerException("bootstrapServers == null");
      adminConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      aggregationStreamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      traceStoreStreamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      dependencyStoreStreamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      return this;
    }

    public Builder aggregationStreamAppId(String aggregationStreamAppId) {
      if (aggregationStreamAppId == null) {
        throw new NullPointerException("aggregationStreamAppId == null");
      }
      this.aggregationStreamAppId = aggregationStreamAppId;
      aggregationStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, aggregationStreamAppId);
      return this;
    }

    public Builder traceStoreStreamAppId(String traceStoreStreamAppId) {
      if (traceStoreStreamAppId == null) {
        throw new NullPointerException("traceStoreStreamAppId == null");
      }
      this.traceStoreStreamAppId = traceStoreStreamAppId;
      traceStoreStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, traceStoreStreamAppId);
      return this;
    }

    public Builder dependencyStoreStreamAppId(String dependencyStoreStreamAppId) {
      if (dependencyStoreStreamAppId == null) {
        throw new NullPointerException("dependencyStoreStreamAppId == null");
      }
      this.dependencyStoreStreamAppId = dependencyStoreStreamAppId;
      dependencyStoreStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,
          dependencyStoreStreamAppId);
      return this;
    }

    /**
     * Kafka topic name where incoming spans are stored.
     * <p>
     * A Span is received from Collectors that contains all metadata and is partitioned by Trace
     * Id.
     */
    public Builder spansTopicName(String spansTopicName) {
      if (spansTopicName == null) throw new NullPointerException("spansTopicName == null");
      this.spansTopicName = spansTopicName;
      return this;
    }

    /**
     * Kafka topic name where incoming spans are stored.
     * <p>
     * A Span is received from Collectors that contains all metadata and is partitioned by Trace
     * Id.
     */
    public Builder tracesTopicName(String tracesTopicName) {
      if (tracesTopicName == null) throw new NullPointerException("tracesTopicName == null");
      this.traceTopicName = tracesTopicName;
      return this;
    }

    /**
     * Kafka topic name where dependencies changelog are stored.
     */
    public Builder dependenciesTopicName(String dependenciesTopicName) {
      if (dependenciesTopicName == null) {
        throw new NullPointerException("dependenciesTopicName == null");
      }
      this.dependencyTopicName = dependenciesTopicName;
      return this;
    }

    /**
     * Path to root directory when aggregated and indexed data is stored.
     */
    public Builder storageDir(String storageDir) {
      if (storageDir == null) throw new NullPointerException("storageDir == null");
      this.storageDir = storageDir;
      traceStoreStreamConfig.put(StreamsConfig.STATE_DIR_CONFIG, traceStoreDirectory());
      dependencyStoreStreamConfig.put(StreamsConfig.STATE_DIR_CONFIG, dependencyStoreDirectory());
      return this;
    }

    /**
     * Frequency to check retention policy.
     */
    public Builder traceTtlCheckInterval(Duration traceTtlCheckInterval) {
      if (traceTtlCheckInterval == null) {
        throw new NullPointerException("traceTtlCheckInterval == null");
      }
      this.traceTtlCheckInterval = traceTtlCheckInterval;
      return this;
    }

    /**
     * Traces time-to-live on local state stores.
     */
    public Builder traceTtl(Duration traceTtl) {
      if (this.traceTtl == null) throw new NullPointerException("traceTtl == null");
      this.traceTtl = traceTtl;
      return this;
    }

    /**
     * Dependencies time-to-live on local state stores.
     */
    public Builder dependencyTtl(Duration dependencyTtl) {
      if (dependencyTtl == null) throw new NullPointerException("dependencyTtl == null");
      this.dependencyTtl = dependencyTtl;
      return this;
    }

    String traceStoreDirectory() {
      return storageDir + "/traces";
    }

    String dependencyStoreDirectory() {
      return storageDir + "/dependencies";
    }

    /**
     * By default, an Admin Client will be built from properties derived from builder defaults, as
     * well as "client.id" -> "zipkin-storage". Any properties set here will override the admin
     * client config.
     *
     * <p>For example: Set the client ID for the AdminClient.
     *
     * <pre>{@code
     * Map<String, String> overrides = new LinkedHashMap<>();
     * overrides.put(AdminClientConfig.CLIENT_ID_CONFIG, "zipkin-storage");
     * builder.overrides(overrides);
     * }</pre>
     *
     * @see org.apache.kafka.clients.admin.AdminClientConfig
     */
    public final Builder adminOverrides(Map<String, ?> overrides) {
      if (overrides == null) throw new NullPointerException("overrides == null");
      adminConfig.putAll(overrides);
      return this;
    }

    /**
     * By default, a produce will be built from properties derived from builder defaults, as well as
     * "batch.size" -> 1000. Any properties set here will override the consumer config.
     *
     * <p>For example: Only send batch of list of spans with a maximum size of 1000 bytes
     *
     * <pre>{@code
     * Map<String, String> overrides = new LinkedHashMap<>();
     * overrides.put(ProducerConfig.BATCH_SIZE_CONFIG, 1000);
     * builder.overrides(overrides);
     * }</pre>
     *
     * @see org.apache.kafka.clients.producer.ProducerConfig
     */
    public final Builder producerOverrides(Map<String, ?> overrides) {
      if (overrides == null) throw new NullPointerException("overrides == null");
      producerConfig.putAll(overrides);
      return this;
    }

    /**
     * By default, a Kafka Streams applications will be built from properties derived from builder
     * defaults, as well as "poll.ms" -> 5000. Any properties set here will override the Kafka
     * Streams application config.
     *
     * <p>For example: to change the Streams poll timeout:
     *
     * <pre>{@code
     * Map<String, String> overrides = new LinkedHashMap<>();
     * overrides.put(StreamsConfig.POLL_MS, 5000);
     * builder.aggregationStreamOverrides(overrides);
     * }</pre>
     *
     * @see org.apache.kafka.streams.StreamsConfig
     */
    public final Builder aggregationStreamOverrides(Map<String, ?> overrides) {
      if (overrides == null) throw new NullPointerException("overrides == null");
      aggregationStreamConfig.putAll(overrides);
      return this;
    }

    /**
     * By default, a Kafka Streams applications will be built from properties derived from builder
     * defaults, as well as "poll.ms" -> 5000. Any properties set here will override the Kafka
     * Streams application config.
     *
     * <p>For example: to change the Streams poll timeout:
     *
     * <pre>{@code
     * Map<String, String> overrides = new LinkedHashMap<>();
     * overrides.put(StreamsConfig.POLL_MS, 5000);
     * builder.traceStoreStreamOverrides(overrides);
     * }</pre>
     *
     * @see org.apache.kafka.streams.StreamsConfig
     */
    public final Builder traceStoreStreamOverrides(Map<String, ?> overrides) {
      if (overrides == null) throw new NullPointerException("overrides == null");
      traceStoreStreamConfig.putAll(overrides);
      return this;
    }

    /**
     * By default, a Kafka Streams applications will be built from properties derived from builder
     * defaults, as well as "poll.ms" -> 5000. Any properties set here will override the Kafka
     * Streams application config.
     *
     * <p>For example: to change the Streams poll timeout:
     *
     * <pre>{@code
     * Map<String, String> overrides = new LinkedHashMap<>();
     * overrides.put(StreamsConfig.POLL_MS, 5000);
     * builder.dependencyStoreStreamOverrides(overrides);
     * }</pre>
     *
     * @see org.apache.kafka.streams.StreamsConfig
     */
    public final Builder dependencyStoreStreamOverrides(Map<String, ?> overrides) {
      if (overrides == null) throw new NullPointerException("overrides == null");
      dependencyStoreStreamConfig.putAll(overrides);
      return this;
    }

    @Override
    public StorageComponent build() {
      return new KafkaStorage(this);
    }
  }

  @Override public String toString() {
    return "KafkaStorage{" +
        "httpPort=" + httpPort +
        ", spanConsumerEnabled=" + spanConsumerEnabled +
        ", searchEnabled=" + searchEnabled +
        ", storageDir='" + storageDir + '\'' +
        ", spansTopicName='" + spansTopicName + '\'' +
        ", traceTopicName='" + traceTopicName + '\'' +
        ", dependencyTopicName='" + dependencyTopicName + '\'' +
        '}';
  }
}
