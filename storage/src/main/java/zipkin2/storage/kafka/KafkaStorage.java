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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.record.CompressionType;
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
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.storage.*;
import zipkin2.storage.kafka.streams.AggregationTopologySupplier;
import zipkin2.storage.kafka.streams.StoreTopologySupplier;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Zipkin's Kafka Storage.
 * <p>
 * Storage implementation based on Kafka Streams, supporting:
 * <ul>
 *   <li>repartitioning of spans,</li>
 *   <li>trace aggregation,</li>
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
  final String storageDirectory;
  // Kafka Topics
  final String spansTopicName, tracesTopicName, dependenciesTopicName;
  // Kafka Clients config
  final Properties adminConfig;
  final Properties producerConfig;
  // Kafka Streams topology configs
  final Properties aggregationStreamConfig, storeStreamConfig;
  final Topology aggregationTopology, storeTopology;
  // Resources
  volatile AdminClient adminClient;
  volatile Producer<String, byte[]> producer;
  volatile KafkaStreams traceAggregationStream, traceStoreStream;
  volatile boolean closeCalled, topicsValidated;

  KafkaStorage(Builder builder) {
    // Kafka Storage modes
    this.spanConsumerEnabled = builder.spanConsumerEnabled;
    this.searchEnabled = builder.searchEnabled;
    // Autocomplete tags
    this.autocompleteKeys = builder.autocompleteKeys;
    // Kafka Topics config
    this.spansTopicName = builder.spansTopicName;
    this.tracesTopicName = builder.tracesTopicName;
    this.dependenciesTopicName = builder.dependenciesTopicName;
    // State store directories
    this.storageDirectory = builder.storeDir;
    // Kafka Configs
    this.adminConfig = builder.adminConfig;
    this.producerConfig = builder.producerConfig;
    this.aggregationStreamConfig = builder.aggregationStreamConfig;
    this.storeStreamConfig = builder.storeStreamConfig;

    aggregationTopology = new AggregationTopologySupplier(spansTopicName, tracesTopicName,
        dependenciesTopicName, builder.tracesInactivityGap).get();
    storeTopology = new StoreTopologySupplier(
        tracesTopicName,
        dependenciesTopicName,
        autocompleteKeys,
        builder.tracesRetentionScanFrequency,
        builder.tracesRetentionPeriod,
        builder.dependenciesRetentionPeriod,
        builder.dependenciesWindowSize).get();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public SpanConsumer spanConsumer() {
    checkTopics();
    if (spanConsumerEnabled) {
      getAggregationStream();
      return new KafkaSpanConsumer(this);
    } else { // NoopSpanConsumer
      return list -> Call.create(null);
    }
  }

  @Override
  public ServiceAndSpanNames serviceAndSpanNames() {
    if (searchEnabled) {
      return new KafkaSpanStore(this);
    } else { // NoopServiceAndSpanNames
      return new ServiceAndSpanNames() {
        @Override public Call<List<String>> getServiceNames() {
          return Call.emptyList();
        }

        @Override public Call<List<String>> getRemoteServiceNames(String serviceName) {
          return Call.emptyList();
        }

        @Override public Call<List<String>> getSpanNames(String s) {
          return Call.emptyList();
        }
      };
    }
  }

  @Override
  public SpanStore spanStore() {
    checkTopics();
    if (searchEnabled) {
      return new KafkaSpanStore(this);
    } else { // NoopSpanStore
      return new SpanStore() {
        @Override public Call<List<List<Span>>> getTraces(QueryRequest queryRequest) {
          return Call.emptyList();
        }

        @Override public Call<List<Span>> getTrace(String s) {
          return Call.emptyList();
        }

        @Override @Deprecated public Call<List<String>> getServiceNames() {
          return Call.emptyList();
        }

        @Override @Deprecated public Call<List<String>> getSpanNames(String s) {
          return Call.emptyList();
        }

        @Override public Call<List<DependencyLink>> getDependencies(long l, long l1) {
          return Call.emptyList();
        }
      };
    }
  }

  @Override public AutocompleteTags autocompleteTags() {
    checkTopics();
    if (searchEnabled) {
      return new KafkaAutocompleteTags(this);
    } else {
      return super.autocompleteTags();
    }
  }

  /**
   * Ensure topics are created before Kafka Streams applications start.
   * <p>
   * It is recommended to created these topics manually though, before application is started.
   */
  void checkTopics() {
    if (!topicsValidated) {
      synchronized (this) {
        if (!topicsValidated) {
          try {
            Set<String> topics = getAdminClient().listTopics().names().get(1, TimeUnit.SECONDS);
            List<String> requiredTopics =
                Arrays.asList(spansTopicName, dependenciesTopicName, tracesTopicName);
            for (String requiredTopic : requiredTopics) {
              if (!topics.contains(requiredTopic)) {
                throw new RuntimeException("Required topics are not created");
              }
            }
            topicsValidated = true;
          } catch (Exception e) {
            LOG.error("Error ensuring topics are created", e);
          }
        }
      }
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
        KafkaStreams.State state = getStoreStream().state();
        if (!state.isRunning()) {
          return CheckResult.failed(
              new IllegalStateException("Store stream not running. " + state));
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
        producer.flush();
        producer.close(Duration.ofSeconds(1));
      }
      if (traceStoreStream != null) {
        traceStoreStream.close(Duration.ofSeconds(1));
      }
      if (traceAggregationStream != null) {
        traceAggregationStream.close(Duration.ofSeconds(1));
      }
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

  KafkaStreams getStoreStream() {
    if (traceStoreStream == null) {
      synchronized (this) {
        if (traceStoreStream == null) {
          traceStoreStream = new KafkaStreams(storeTopology, storeStreamConfig);
          traceStoreStream.start();
        }
      }
    }
    return traceStoreStream;
  }

  KafkaStreams getAggregationStream() {
    if (traceAggregationStream == null) {
      synchronized (this) {
        if (traceAggregationStream == null) {
          traceAggregationStream =
              new KafkaStreams(aggregationTopology, aggregationStreamConfig);
          traceAggregationStream.start();
        }
      }
    }
    return traceAggregationStream;
  }

  public static class Builder extends StorageComponent.Builder {
    boolean spanConsumerEnabled = true;
    boolean searchEnabled = true;

    List<String> autocompleteKeys = new ArrayList<>();

    Duration tracesRetentionPeriod = Duration.ofDays(7);
    Duration tracesRetentionScanFrequency = Duration.ofHours(1);
    Duration tracesInactivityGap = Duration.ofSeconds(30);
    Duration dependenciesRetentionPeriod = Duration.ofDays(7);
    Duration dependenciesWindowSize = Duration.ofMinutes(1);

    String storeDir = "/tmp/zipkin";

    Properties adminConfig = new Properties();
    Properties producerConfig = new Properties();
    Properties aggregationStreamConfig = new Properties();
    Properties storeStreamConfig = new Properties();

    //TODO expose as configs
    String traceStoreStreamAppId = "zipkin-trace-store";
    String aggregationStreamAppId = "zipkin-trace-aggregation";

    String spansTopicName = "zipkin-spans";
    String tracesTopicName = "zipkin-traces";
    String dependenciesTopicName = "zipkin-dependencies";

    Builder() {
      // Kafka Producer configuration
      producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
      producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
      producerConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, CompressionType.SNAPPY.name);
      producerConfig.put(ProducerConfig.BATCH_SIZE_CONFIG, 500_000);
      producerConfig.put(ProducerConfig.LINGER_MS_CONFIG, 100);
      aggregationStreamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
          Serdes.StringSerde.class);
      aggregationStreamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
          Serdes.ByteArraySerde.class);
      aggregationStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, aggregationStreamAppId);
      aggregationStreamConfig.put(StreamsConfig.STATE_DIR_CONFIG, traceStoreDirectory());
      aggregationStreamConfig.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
      aggregationStreamConfig.put(
          StreamsConfig.PRODUCER_PREFIX + ProducerConfig.COMPRESSION_TYPE_CONFIG,
          CompressionType.SNAPPY.name);
      // Trace Store Stream Topology configuration
      storeStreamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
      storeStreamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
          Serdes.ByteArraySerde.class);
      storeStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, traceStoreStreamAppId);
      storeStreamConfig.put(StreamsConfig.STATE_DIR_CONFIG, traceStoreDirectory());
      storeStreamConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, CompressionType.SNAPPY.name);
      storeStreamConfig.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
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

    /**
     * How long to wait for a span in order to trigger a trace as completed.
     */
    public Builder tracesInactivityGap(Duration tracesInactivityGap) {
      if (tracesInactivityGap == null) {
        throw new NullPointerException("tracesInactivityGap == null");
      }
      this.tracesInactivityGap = tracesInactivityGap;
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
      storeStreamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
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
      this.tracesTopicName = tracesTopicName;
      return this;
    }

    /**
     * Kafka topic name where dependencies changelog are stored.
     */
    public Builder dependenciesTopicName(String dependenciesTopicName) {
      if (dependenciesTopicName == null) {
        throw new NullPointerException("dependenciesTopicName == null");
      }
      this.dependenciesTopicName = dependenciesTopicName;
      return this;
    }

    /**
     * Path to root directory when aggregated and indexed data is stored.
     */
    public Builder storeDirectory(String storeDirectory) {
      if (storeDirectory == null) throw new NullPointerException("storageDirectory == null");
      this.storeDir = storeDirectory;
      return this;
    }

    /**
     * Frequency to check retention policy.
     */
    public Builder tracesRetentionScanFrequency(Duration tracesRetentionScanFrequency) {
      this.tracesRetentionScanFrequency = tracesRetentionScanFrequency;
      return this;
    }

    /**
     * Maximum age for traces and spans to be retained on State Stores.
     */
    public Builder tracesRetentionRetention(Duration tracesRetentionMaxAge) {
      this.tracesRetentionPeriod = tracesRetentionMaxAge;
      return this;
    }

    /**
     * Retention period for Dependencies.
     */
    public Builder dependenciesRetentionPeriod(Duration dependenciesRetentionPeriod) {
      this.dependenciesRetentionPeriod = dependenciesRetentionPeriod;
      return this;
    }

    /**
     * Dependencies store window size
     */
    public Builder dependenciesWindowSize(Duration dependenciesWindowSize) {
      this.dependenciesWindowSize = dependenciesWindowSize;
      return this;
    }

    String traceStoreDirectory() {
      return storeDir + "/streams/traces";
    }

    /**
     * By default, a consumer will be built from properties derived from builder defaults, as well
     * as "auto.offset.reset" -> "earliest". Any properties set here will override the consumer
     * config.
     *
     * <p>For example: Only consume spans since you connected by setting the below.
     *
     * <pre>{@code
     * Map<String, String> overrides = new LinkedHashMap<>();
     * overrides.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
     * builder.overrides(overrides);
     * }</pre>
     *
     * @see org.apache.kafka.clients.consumer.ConsumerConfig
     */
    public final Builder adminOverrides(Map<String, ?> overrides) {
      if (overrides == null) throw new NullPointerException("overrides == null");
      adminConfig.putAll(overrides);
      return this;
    }

    /**
     * By default, a consumer will be built from properties derived from builder defaults, as well
     * as "auto.offset.reset" -> "earliest". Any properties set here will override the consumer
     * config.
     *
     * <p>For example: Only consume spans since you connected by setting the below.
     *
     * <pre>{@code
     * Map<String, String> overrides = new LinkedHashMap<>();
     * overrides.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
     * builder.overrides(overrides);
     * }</pre>
     *
     * @see org.apache.kafka.clients.consumer.ConsumerConfig
     */
    public final Builder producerOverrides(Map<String, ?> overrides) {
      if (overrides == null) throw new NullPointerException("overrides == null");
      producerConfig.putAll(overrides);
      return this;
    }

    /**
     * By default, a consumer will be built from properties derived from builder defaults, as well
     * as "auto.offset.reset" -> "earliest". Any properties set here will override the consumer
     * config.
     *
     * <p>For example: Only consume spans since you connected by setting the below.
     *
     * <pre>{@code
     * Map<String, String> overrides = new LinkedHashMap<>();
     * overrides.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
     * builder.overrides(overrides);
     * }</pre>
     *
     * @see org.apache.kafka.clients.consumer.ConsumerConfig
     */
    public final Builder aggregationStreamOverrides(Map<String, ?> overrides) {
      if (overrides == null) throw new NullPointerException("overrides == null");
      aggregationStreamConfig.putAll(overrides);
      return this;
    }

    /**
     * By default, a consumer will be built from properties derived from builder defaults, as well
     * as "auto.offset.reset" -> "earliest". Any properties set here will override the consumer
     * config.
     *
     * <p>For example: Only consume spans since you connected by setting the below.
     *
     * <pre>{@code
     * Map<String, String> overrides = new LinkedHashMap<>();
     * overrides.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
     * builder.overrides(overrides);
     * }</pre>
     *
     * @see org.apache.kafka.clients.consumer.ConsumerConfig
     */
    public final Builder storeStreamOverrides(Map<String, ?> overrides) {
      if (overrides == null) throw new NullPointerException("overrides == null");
      storeStreamConfig.putAll(overrides);
      return this;
    }

    @Override
    public StorageComponent build() {
      return new KafkaStorage(this);
    }
  }
}
