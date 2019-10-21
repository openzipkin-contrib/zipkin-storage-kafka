/*
 * Copyright 2019 The OpenZipkin Authors
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiFunction;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import zipkin2.storage.StorageComponent;

import static zipkin2.storage.kafka.KafkaStorage.HTTP_PATH_PREFIX;

// extracted as the type is huge
public final class KafkaStorageBuilder extends StorageComponent.Builder {
  boolean spanConsumerEnabled = true;
  boolean aggregationEnabled = true;
  boolean traceByIdQueryEnabled = true;
  boolean traceSearchEnabled = true;
  boolean dependencyQueryEnabled = true;

  List<String> autocompleteKeys = new ArrayList<>();

  Duration traceTtl = Duration.ofDays(3);
  Duration traceTtlCheckInterval = Duration.ofHours(1);
  Duration traceTimeout = Duration.ofMinutes(1);
  Duration dependencyTtl = Duration.ofDays(7);
  Duration dependencyWindowSize = Duration.ofMinutes(1);

  long minTracesStored = 10_000;
  String hostname = "localhost";
  int httpPort = 9411;
  BiFunction<String, Integer, String> httpBaseUrl =
      (hostname, port) -> "http://" + hostname + ":" + port + HTTP_PATH_PREFIX;

  String storageDir = "/tmp/zipkin-storage-kafka";

  Properties adminConfig = new Properties();
  Properties producerConfig = new Properties();
  Properties aggregationStreamConfig = new Properties();
  Properties traceStoreStreamConfig = new Properties();
  Properties dependencyStoreStreamConfig = new Properties();

  String traceStoreStreamAppId = "zipkin-trace-store";
  String dependencyStoreStreamAppId = "zipkin-dependency-store";
  String aggregationStreamAppId = "zipkin-aggregation";

  String partitionedSpansTopic = "zipkin-spans";
  String aggregationSpansTopic = "zipkin-spans";
  String aggregationTraceTopic = "zipkin-trace";
  String aggregationDependencyTopic = "zipkin-dependency";
  String storageSpansTopic = "zipkin-spans";
  String storageDependencyTopic = "zipkin-dependency";

  KafkaStorageBuilder() {
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
    return hostname + ":" + httpPort;
  }

  @Override public KafkaStorageBuilder strictTraceId(boolean strictTraceId) {
    if (!strictTraceId) throw new IllegalArgumentException("non-strict trace ID not supported");
    return this;
  }

  @Override public KafkaStorageBuilder searchEnabled(boolean searchEnabled) {
    this.traceSearchEnabled = searchEnabled;
    return this;
  }

  @Override public KafkaStorageBuilder autocompleteKeys(List<String> keys) {
    if (keys == null) throw new NullPointerException("keys == null");
    this.autocompleteKeys = keys;
    return this;
  }

  public KafkaStorageBuilder spanConsumerEnabled(boolean spanConsumerEnabled) {
    this.spanConsumerEnabled = spanConsumerEnabled;
    return this;
  }

  public KafkaStorageBuilder aggregationEnabled(boolean aggregationEnabled) {
    this.aggregationEnabled = aggregationEnabled;
    return this;
  }

  public KafkaStorageBuilder traceByIdQueryEnabled(boolean traceByIdQueryEnabled) {
    this.traceByIdQueryEnabled = traceByIdQueryEnabled;
    return this;
  }

  public KafkaStorageBuilder traceSearchEnabled(boolean traceSearchEnabled) {
    this.traceSearchEnabled = traceSearchEnabled;
    return this;
  }

  public KafkaStorageBuilder dependencyQueryEnabled(boolean dependencyQueryEnabled) {
    this.dependencyQueryEnabled = dependencyQueryEnabled;
    return this;
  }

  public KafkaStorageBuilder hostname(String hostname) {
    if (hostname == null) throw new NullPointerException("hostname == null");
    this.hostname = hostname;
    traceStoreStreamConfig.put(StreamsConfig.APPLICATION_SERVER_CONFIG, hostInfo());
    dependencyStoreStreamConfig.put(StreamsConfig.APPLICATION_SERVER_CONFIG, hostInfo());
    return this;
  }

  public KafkaStorageBuilder serverPort(int httpPort) {
    this.httpPort = httpPort;
    traceStoreStreamConfig.put(StreamsConfig.APPLICATION_SERVER_CONFIG, hostInfo());
    dependencyStoreStreamConfig.put(StreamsConfig.APPLICATION_SERVER_CONFIG, hostInfo());
    return this;
  }

  /**
   * How long to wait for a span in order to trigger a trace as completed.
   */
  public KafkaStorageBuilder traceTimeout(Duration traceTimeout) {
    if (traceTimeout == null) throw new NullPointerException("traceTimeout == null");
    this.traceTimeout = traceTimeout;
    return this;
  }

  /**
   * Kafka Bootstrap Servers list to establish connection with a Cluster.
   */
  public KafkaStorageBuilder bootstrapServers(String bootstrapServers) {
    if (bootstrapServers == null) throw new NullPointerException("bootstrapServers == null");
    adminConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    aggregationStreamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    traceStoreStreamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    dependencyStoreStreamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    return this;
  }

  public KafkaStorageBuilder aggregationStreamAppId(String aggregationStreamAppId) {
    if (aggregationStreamAppId == null) {
      throw new NullPointerException("aggregationStreamAppId == null");
    }
    this.aggregationStreamAppId = aggregationStreamAppId;
    aggregationStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, aggregationStreamAppId);
    return this;
  }

  public KafkaStorageBuilder traceStoreStreamAppId(String traceStoreStreamAppId) {
    if (traceStoreStreamAppId == null) {
      throw new NullPointerException("traceStoreStreamAppId == null");
    }
    this.traceStoreStreamAppId = traceStoreStreamAppId;
    traceStoreStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, traceStoreStreamAppId);
    return this;
  }

  public KafkaStorageBuilder dependencyStoreStreamAppId(String dependencyStoreStreamAppId) {
    if (dependencyStoreStreamAppId == null) {
      throw new NullPointerException("dependencyStoreStreamAppId == null");
    }
    this.dependencyStoreStreamAppId = dependencyStoreStreamAppId;
    dependencyStoreStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,
        dependencyStoreStreamAppId);
    return this;
  }

  /**
   * Kafka topic name where incoming partitioned spans are stored.
   * <p>
   * A Span is received from Collectors that contains all metadata and is partitioned by Trace Id.
   */
  public KafkaStorageBuilder partitionedSpansTopic(String partitionedSpansTopic) {
    if (partitionedSpansTopic == null) {
      throw new NullPointerException("partitionedSpansTopic == null");
    }
    this.partitionedSpansTopic = partitionedSpansTopic;
    return this;
  }

  /**
   * Kafka topic name where partitioned spans are stored to be used on aggregation.
   */
  public KafkaStorageBuilder aggregationSpansTopic(String aggregationSpansTopic) {
    if (aggregationSpansTopic == null) {
      throw new NullPointerException("aggregationSpansTopic == null");
    }
    this.aggregationSpansTopic = aggregationSpansTopic;
    return this;
  }

  /**
   * Kafka topic name where aggregated traces are stored.
   * <p>
   * Topic with key = traceId and value = list of Spans.
   */
  public KafkaStorageBuilder aggregationTraceTopic(String aggregationTraceTopic) {
    if (aggregationTraceTopic == null) {
      throw new NullPointerException("aggregationTraceTopic == null");
    }
    this.aggregationTraceTopic = aggregationTraceTopic;
    return this;
  }

  /**
   * Kafka topic name where dependencies changelog are stored.
   * <p>
   * Topic with key = parent-child pair and value = dependency link
   */
  public KafkaStorageBuilder aggregationDependencyTopic(String aggregationDependencyTopic) {
    if (aggregationDependencyTopic == null) {
      throw new NullPointerException("aggregationDependencyTopic == null");
    }
    this.aggregationDependencyTopic = aggregationDependencyTopic;
    return this;
  }

  /**
   * Kafka topic name where partitioned spans are stored to be used on aggregation.
   */
  public KafkaStorageBuilder storageSpansTopic(String storageSpansTopic) {
    if (storageSpansTopic == null) throw new NullPointerException("storageSpansTopic == null");
    this.storageSpansTopic = storageSpansTopic;
    return this;
  }

  /**
   * Kafka topic name where dependencies changelog are stored.
   */
  public KafkaStorageBuilder storageDependencyTopic(String storageDependencyTopic) {
    if (storageDependencyTopic == null) {
      throw new NullPointerException("storageDependencyTopic == null");
    }
    this.storageDependencyTopic = storageDependencyTopic;
    return this;
  }

  /**
   * Path to root directory when aggregated and indexed data is stored.
   */
  public KafkaStorageBuilder storageDir(String storageDir) {
    if (storageDir == null) throw new NullPointerException("storageDir == null");
    this.storageDir = storageDir;
    traceStoreStreamConfig.put(StreamsConfig.STATE_DIR_CONFIG, traceStoreDirectory());
    dependencyStoreStreamConfig.put(StreamsConfig.STATE_DIR_CONFIG, dependencyStoreDirectory());
    return this;
  }

  /**
   * Frequency to check retention policy.
   */
  public KafkaStorageBuilder traceTtlCheckInterval(Duration traceTtlCheckInterval) {
    if (traceTtlCheckInterval == null) {
      throw new NullPointerException("traceTtlCheckInterval == null");
    }
    this.traceTtlCheckInterval = traceTtlCheckInterval;
    return this;
  }

  /**
   * Traces time-to-live on local state stores.
   */
  public KafkaStorageBuilder traceTtl(Duration traceTtl) {
    if (this.traceTtl == null) throw new NullPointerException("traceTtl == null");
    this.traceTtl = traceTtl;
    return this;
  }

  /**
   * Dependencies time-to-live on local state stores.
   */
  public KafkaStorageBuilder dependencyTtl(Duration dependencyTtl) {
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
   * well as "client.id" -> "zipkin-storage". Any properties set here will override the admin client
   * config.
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
  public final KafkaStorageBuilder adminOverrides(Map<String, ?> overrides) {
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
  public final KafkaStorageBuilder producerOverrides(Map<String, ?> overrides) {
    if (overrides == null) throw new NullPointerException("overrides == null");
    producerConfig.putAll(overrides);
    return this;
  }

  /**
   * By default, a Kafka Streams applications will be built from properties derived from builder
   * defaults, as well as "poll.ms" -> 5000. Any properties set here will override the Kafka Streams
   * application config.
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
  public final KafkaStorageBuilder aggregationStreamOverrides(Map<String, ?> overrides) {
    if (overrides == null) throw new NullPointerException("overrides == null");
    aggregationStreamConfig.putAll(overrides);
    return this;
  }

  /**
   * By default, a Kafka Streams applications will be built from properties derived from builder
   * defaults, as well as "poll.ms" -> 5000. Any properties set here will override the Kafka Streams
   * application config.
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
  public final KafkaStorageBuilder traceStoreStreamOverrides(Map<String, ?> overrides) {
    if (overrides == null) throw new NullPointerException("overrides == null");
    traceStoreStreamConfig.putAll(overrides);
    return this;
  }

  /**
   * By default, a Kafka Streams applications will be built from properties derived from builder
   * defaults, as well as "poll.ms" -> 5000. Any properties set here will override the Kafka Streams
   * application config.
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
  public final KafkaStorageBuilder dependencyStoreStreamOverrides(Map<String, ?> overrides) {
    if (overrides == null) throw new NullPointerException("overrides == null");
    dependencyStoreStreamConfig.putAll(overrides);
    return this;
  }

  @Override public StorageComponent build() {
    return new KafkaStorage(this);
  }
}
