/*
 * Copyright 2019-2020 The OpenZipkin Authors
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
import org.apache.kafka.common.serialization.Serdes.ByteArraySerde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import zipkin2.storage.StorageComponent;

import static zipkin2.storage.kafka.KafkaStorage.HTTP_PATH_PREFIX;

// extracted as the type is huge
public final class KafkaStorageBuilder extends StorageComponent.Builder {
  List<String> autocompleteKeys = new ArrayList<>();

  String hostname = "localhost";
  int serverPort = 9411;
  BiFunction<String, Integer, String> httpBaseUrl =
      (hostname, port) -> "http://" + hostname + ":" + port + HTTP_PATH_PREFIX;

  SpanPartitioningBuilder spanPartitioning = new SpanPartitioningBuilder();
  SpanAggregationBuilder spanAggregation = new SpanAggregationBuilder();
  TraceStorageBuilder traceStorage = new TraceStorageBuilder();
  DependencyStorageBuilder dependencyStorage = new DependencyStorageBuilder();

  Properties adminConfig = new Properties();

  KafkaStorageBuilder() {
  }

  @Override public KafkaStorageBuilder strictTraceId(boolean strictTraceId) {
    if (!strictTraceId) throw new IllegalArgumentException("non-strict trace ID not supported");
    return this;
  }

  @Override public KafkaStorageBuilder searchEnabled(boolean searchEnabled) {
    traceStorage.searchEnabled(searchEnabled);
    return this;
  }

  @Override public KafkaStorageBuilder autocompleteKeys(List<String> keys) {
    if (keys == null) throw new NullPointerException("keys == null");
    this.autocompleteKeys = keys;
    return this;
  }

  /**
   * Kafka Bootstrap Servers list to establish connection with a Cluster.
   */
  public KafkaStorageBuilder bootstrapServers(String bootstrapServers) {
    if (bootstrapServers == null) throw new NullPointerException("bootstrapServers == null");
    adminConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    spanPartitioning.bootstrapServers(bootstrapServers);
    spanAggregation.bootstrapServers(bootstrapServers);
    traceStorage.bootstrapServers(bootstrapServers);
    dependencyStorage.bootstrapServers(bootstrapServers);
    return this;
  }

  /**
   * Path to root directory when aggregated and indexed data is stored.
   */
  public KafkaStorageBuilder storageStateDir(String storageStateDir) {
    if (storageStateDir == null) throw new NullPointerException("storageStateDir == null");
    spanAggregation.storageStateDir(storageStateDir);
    traceStorage.storageStateDir(storageStateDir);
    dependencyStorage.storageStateDir(storageStateDir);
    return this;
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
  public final KafkaStorageBuilder overrides(Map<String, ?> overrides) {
    if (overrides == null) throw new NullPointerException("overrides == null");
    adminConfig.putAll(overrides);
    spanPartitioning.overrides(overrides);
    spanAggregation.overrides(overrides);
    traceStorage.overrides(overrides);
    dependencyStorage.overrides(overrides);
    return this;
  }

  /**
   * Use this hostname to locate zipkin server between each other when forming a cluster.
   * <p>
   * When running multiple instances server local IP might not be the same as the external IP. e.g.
   * Kubernetes Pod IP not been accessible from other Pods, and Service IPs that are accessible.
   */
  public KafkaStorageBuilder hostname(String hostname) {
    if (hostname == null) throw new NullPointerException("hostname == null");
    this.hostname = hostname;
    traceStorage.hostInfo(hostname, serverPort);
    dependencyStorage.hostInfo(hostname, serverPort);
    return this;
  }

  /**
   * Same port as Zipkin Server. To be changed only when Zipkin Server port is changed.
   */
  public KafkaStorageBuilder serverPort(int serverPort) {
    if (serverPort <= 0) throw new IllegalArgumentException("serverPort <= 0");
    this.serverPort = serverPort;
    traceStorage.hostInfo(hostname, serverPort);
    dependencyStorage.hostInfo(hostname, serverPort);
    return this;
  }

  public KafkaStorageBuilder spanPartitioningBuilder(SpanPartitioningBuilder builder) {
    if (builder == null) throw new NullPointerException("builder == null");
    this.spanPartitioning = builder;
    return this;
  }

  public KafkaStorageBuilder spanAggregationBuilder(SpanAggregationBuilder builder) {
    if (builder == null) throw new NullPointerException("builder == null");
    this.spanAggregation = builder;
    return this;
  }

  public KafkaStorageBuilder traceStorageBuilder(TraceStorageBuilder builder) {
    if (builder == null) throw new NullPointerException("builder == null");
    this.traceStorage = builder;
    return this;
  }

  public KafkaStorageBuilder dependencyStorageBuilder(DependencyStorageBuilder builder) {
    if (builder == null) throw new NullPointerException("builder == null");
    this.dependencyStorage = builder;
    return this;
  }

  @Override public StorageComponent build() {
    return new KafkaStorage(this);
  }

  public static class SpanPartitioningBuilder {
    boolean enabled = true;
    Properties producerConfig = new Properties();
    String spansTopic = "zipkin-spans";

    public SpanPartitioningBuilder() {
      producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
      producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
      producerConfig.put(ProducerConfig.BATCH_SIZE_CONFIG, 500_000);
      producerConfig.put(ProducerConfig.LINGER_MS_CONFIG, 5);
    }

    public SpanPartitioningBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    /**
     * Kafka Bootstrap Servers list to establish connection with a Cluster.
     */
    public SpanPartitioningBuilder bootstrapServers(String bootstrapServers) {
      if (bootstrapServers == null) throw new NullPointerException("bootstrapServers == null");
      producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      return this;
    }

    /**
     * Kafka topic name where incoming partitioned spans are stored.
     * <p>
     * A Span is received from Collectors that contains all metadata and is partitioned by Trace
     * Id.
     */
    public SpanPartitioningBuilder spansTopic(String spansTopic) {
      if (spansTopic == null) {
        throw new NullPointerException("spansTopic == null");
      }
      this.spansTopic = spansTopic;
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
    public final SpanPartitioningBuilder overrides(Map<String, ?> overrides) {
      if (overrides == null) throw new NullPointerException("overrides == null");
      producerConfig.putAll(overrides);
      return this;
    }
  }

  public static class SpanAggregationBuilder {
    boolean enabled = true;
    Duration traceTimeout = Duration.ofMinutes(1);
    String spansTopic = "zipkin-spans";
    String traceTopic = "zipkin-trace";
    String dependencyTopic = "zipkin-dependency";

    Properties streamConfig = new Properties();

    public SpanAggregationBuilder() {
      streamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
      streamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, ByteArraySerde.class);
      streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "zipkin-aggregation");
      streamConfig.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/zipkin-storage-kafka/aggregation");
      streamConfig.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
    }

    /**
     * Enable aggregation stream application to run. When disabled spans will not be consumed to
     * produce traces and dependencies.
     */
    public SpanAggregationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    /**
     * How long to wait for a span in order to trigger a trace as completed.
     */
    public SpanAggregationBuilder traceTimeout(Duration traceTimeout) {
      if (traceTimeout == null) throw new NullPointerException("traceTimeout == null");
      this.traceTimeout = traceTimeout;
      return this;
    }

    /**
     * Kafka Bootstrap Servers list to establish connection with a Cluster.
     */
    public SpanAggregationBuilder bootstrapServers(String bootstrapServers) {
      if (bootstrapServers == null) throw new NullPointerException("bootstrapServers == null");
      streamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      return this;
    }

    public SpanAggregationBuilder storageStateDir(String parentStateDir) {
      if (parentStateDir == null) throw new NullPointerException("parentStateDir == null");
      streamConfig.put(StreamsConfig.STATE_DIR_CONFIG, parentStateDir + "/aggregation");
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
    public final SpanAggregationBuilder overrides(Map<String, ?> overrides) {
      if (overrides == null) throw new NullPointerException("overrides == null");
      streamConfig.putAll(overrides);
      return this;
    }

    /**
     * Kafka topic name where partitioned spans are stored to be used on aggregation.
     */
    public SpanAggregationBuilder spansTopic(String spansTopic) {
      if (spansTopic == null) throw new NullPointerException("spansTopic == null");
      this.spansTopic = spansTopic;
      return this;
    }

    /**
     * Kafka topic name where aggregated traces are stored.
     * <p>
     * Topic with key = traceId and value = list of Spans.
     */
    public SpanAggregationBuilder traceTopic(String traceTopic) {
      if (traceTopic == null) throw new NullPointerException("traceTopic == null");
      this.traceTopic = traceTopic;
      return this;
    }

    /**
     * Kafka topic name where dependencies changelog are stored.
     * <p>
     * Topic with key = parent-child pair and value = dependency link
     */
    public SpanAggregationBuilder dependencyTopic(String dependencyTopic) {
      if (dependencyTopic == null) throw new NullPointerException("dependencyTopic == null");
      this.dependencyTopic = dependencyTopic;
      return this;
    }
  }

  public static class TraceStorageBuilder {
    boolean enabled = true;
    boolean traceByIdQueryEnabled = true;
    boolean traceSearchEnabled = true;
    String spansTopic = "zipkin-spans";

    Duration traceTtl = Duration.ofDays(3);
    Duration traceTtlCheckInterval = Duration.ofHours(1);

    Properties streamConfig = new Properties();

    long minTracesStored = 10_000;

    public TraceStorageBuilder() {
      streamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
      streamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, ByteArraySerde.class);
      streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "zipkin-trace-storage");
      streamConfig.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/zipkin-storage-kafka/trace-storage");
      streamConfig.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
      streamConfig.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:9411");
    }

    /**
     * Enable Trace query by specific ID.
     */
    public TraceStorageBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      this.traceByIdQueryEnabled = enabled;
      this.traceSearchEnabled = enabled;
      return this;
    }

    /**
     * Enable trace searching and indexes (service names, span names, etc.). When disabled instance
     * will not store trace indexes. If disabled with @{code traceByIdQueryEnabled} then no trace
     * store will be created.
     */
    public TraceStorageBuilder searchEnabled(boolean searchEnabled) {
      this.traceSearchEnabled = searchEnabled;
      return this;
    }

    /**
     * Kafka topic name where partitioned spans are stored to be used on aggregation.
     */
    public TraceStorageBuilder spansTopic(String spansTopic) {
      if (spansTopic == null) throw new NullPointerException("spansTopic == null");
      this.spansTopic = spansTopic;
      return this;
    }

    /**
     * Frequency to check retention policy.
     */
    public TraceStorageBuilder ttlCheckInterval(Duration ttlCheckInterval) {
      if (ttlCheckInterval == null) throw new NullPointerException("ttlCheckInterval == null");
      this.traceTtlCheckInterval = ttlCheckInterval;
      return this;
    }

    /**
     * Traces time-to-live on local state stores.
     */
    public TraceStorageBuilder ttl(Duration ttl) {
      if (this.traceTtl == null) throw new NullPointerException("ttl == null");
      this.traceTtl = ttl;
      return this;
    }

    /**
     * Kafka Bootstrap Servers list to establish connection with a Cluster.
     */
    public TraceStorageBuilder bootstrapServers(String bootstrapServers) {
      if (bootstrapServers == null) throw new NullPointerException("bootstrapServers == null");
      streamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      return this;
    }

    public TraceStorageBuilder storageStateDir(String parentDir) {
      if (parentDir == null) throw new NullPointerException("parentDir == null");
      streamConfig.put(StreamsConfig.STATE_DIR_CONFIG, parentDir + "/traces");
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
    public final TraceStorageBuilder overrides(Map<String, ?> overrides) {
      if (overrides == null) throw new NullPointerException("overrides == null");
      streamConfig.putAll(overrides);
      return this;
    }

    public TraceStorageBuilder hostInfo(String hostname, int port) {
      this.streamConfig.put(StreamsConfig.APPLICATION_SERVER_CONFIG, hostname + ":" + port);
      return this;
    }
  }

  public static class DependencyStorageBuilder {
    boolean enabled = true;
    String dependencyTopic = "zipkin-dependency";

    Duration dependencyTtl = Duration.ofDays(7);
    Duration dependencyWindowSize = Duration.ofMinutes(1);

    Properties streamConfig = new Properties();

    public DependencyStorageBuilder() {
      // Dependency Store Stream Topology configuration
      streamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
      streamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, ByteArraySerde.class);
      streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "zipkin-dependency-storage");
      streamConfig.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/zipkin-storage-kafka/dependency-storage");
      streamConfig.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
      streamConfig.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:9411");
    }

    /**
     * Enable dependency store from aggregated topic and query endpoint.
     */
    public DependencyStorageBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    /**
     * Kafka topic name where dependencies changelog are stored.
     */
    public DependencyStorageBuilder dependencyTopic(String dependencyTopic) {
      if (dependencyTopic == null) throw new NullPointerException("dependencyTopic == null");
      this.dependencyTopic = dependencyTopic;
      return this;
    }

    /**
     * Dependencies time-to-live on local state stores.
     */
    public DependencyStorageBuilder ttl(Duration ttl) {
      if (ttl == null) throw new NullPointerException("ttl == null");
      this.dependencyTtl = ttl;
      return this;
    }

    /**
     * Kafka Bootstrap Servers list to establish connection with a Cluster.
     */
    public DependencyStorageBuilder bootstrapServers(String bootstrapServers) {
      if (bootstrapServers == null) throw new NullPointerException("bootstrapServers == null");
      streamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      return this;
    }

    public DependencyStorageBuilder hostInfo(String hostname, int port) {
      this.streamConfig.put(StreamsConfig.APPLICATION_SERVER_CONFIG, hostname + ":" + port);
      return this;
    }

    public DependencyStorageBuilder storageStateDir(String parentDir) {
      if (parentDir == null) throw new NullPointerException("parentDir == null");
      streamConfig.put(StreamsConfig.STATE_DIR_CONFIG, parentDir + "/dependencies");
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
    public final DependencyStorageBuilder overrides(Map<String, ?> overrides) {
      if (overrides == null) throw new NullPointerException("overrides == null");
      streamConfig.putAll(overrides);
      return this;
    }
  }
}
