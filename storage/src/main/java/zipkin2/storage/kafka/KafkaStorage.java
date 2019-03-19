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

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.TopicConfig;
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
import zipkin2.storage.QueryRequest;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StorageComponent;
import zipkin2.storage.kafka.streams.DependencyStoreStream;
import zipkin2.storage.kafka.streams.ServiceStoreStream;
import zipkin2.storage.kafka.streams.SpanConsumerStream;
import zipkin2.storage.kafka.streams.SpanIndexStream;
import zipkin2.storage.kafka.streams.TraceAggregationStream;
import zipkin2.storage.kafka.streams.TraceRetentionStoreStream;
import zipkin2.storage.kafka.streams.TraceStoreStream;

/**
 * Kafka Storage entry-point.
 *
 * Storage implementation based on Kafka Streams State Stores, supporting aggregation of spans,
 * indexing of traces and retention management.
 */
public class KafkaStorage extends StorageComponent {
  static final Logger LOG = LoggerFactory.getLogger(KafkaStorage.class);

  // Kafka Storage modes
  final boolean spanConsumerEnabled, spanStoreEnabled;

  final boolean ensureTopics;

  // Kafka Storage configs
  final String storageDirectory, traceStoreName, spanIndexStoreName, dependencyStoreName,
      serviceStoreName;


  // Kafka Topics
  final Topic spansTopic, traceSpansTopic, tracesTopic, dependenciesTopic;

  // Kafka Clients config
  final Properties adminConfig;
  final Properties producerConfig;

  // Kafka Streams topology configs
  final Properties spanConsumerStreamConfig, traceStoreStreamConfig, traceAggregationStreamConfig,
      spanIndexStreamConfig, serviceStoreStreamConfig, dependencyStoreStreamConfig,
      traceRetentionStoreStreamConfig;
  final Topology spanConsumerTopology, traceStoreTopology, traceAggregationTopology,
      spanIndexTopology, serviceStoreTopology, dependencyStoreTopology, traceRetentionStoreTopology;

  final Duration traceInactivityGap;

  // Resources
  volatile AdminClient adminClient;
  volatile Producer<String, byte[]> producer;
  volatile KafkaStreams spanConsumerStream, traceStoreStream, traceAggregationStream,
      spanIndexStream, serviceStoreStream, dependencyStoreStream, traceRetentionStoreStream;
  volatile boolean closeCalled;

  KafkaStorage(Builder builder) {
    // Kafka Storage modes
    this.spanConsumerEnabled = builder.spanConsumerEnabled;
    this.spanStoreEnabled = builder.spanStoreEnabled;

    // Kafka Topics config
    this.ensureTopics = builder.ensureTopics;
    this.tracesTopic = builder.tracesTopic;
    this.traceSpansTopic = builder.traceSpansTopic;
    this.spansTopic = builder.spansTopic;
    this.dependenciesTopic = builder.dependenciesTopic;

    // State store directories
    this.storageDirectory = builder.storeDirectory;
    this.spanIndexStoreName = builder.spanIndexStoreName;
    this.traceStoreName = builder.traceStoreName;
    this.dependencyStoreName = builder.dependencyStoreName;
    this.serviceStoreName = builder.serviceStoreName;

    // Kafka Clients configuration
    adminConfig = new Properties();
    adminConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, builder.bootstrapServers);

    producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, builder.bootstrapServers);
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    producerConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, builder.compressionType.name);

    // Aggregation Stream Topology configuration
    spanConsumerStreamConfig = new Properties();
    spanConsumerStreamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, builder.bootstrapServers);
    spanConsumerStreamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.StringSerde.class);
    spanConsumerStreamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.ByteArraySerde.class);
    spanConsumerStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,
        builder.spanConsumerStreamApplicationId);
    spanConsumerStreamConfig.put(StreamsConfig.STATE_DIR_CONFIG,
        builder.spanConsumerStreamStoreDirectory());
    spanConsumerStreamConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
        builder.compressionType.name);
    spanConsumerStreamConfig.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);

    spanConsumerTopology =
        new SpanConsumerStream(spansTopic.name, traceSpansTopic.name).get();

    // Store Stream Topology configuration
    traceStoreStreamConfig = new Properties();
    traceStoreStreamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, builder.bootstrapServers);
    traceStoreStreamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.StringSerde.class);
    traceStoreStreamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.ByteArraySerde.class);
    traceStoreStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,
        builder.traceStoreStreamApplicationId);
    traceStoreStreamConfig.put(StreamsConfig.STATE_DIR_CONFIG,
        builder.traceStoreStreamStoreDirectory());
    traceStoreStreamConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
        builder.compressionType.name);
    traceStoreStreamConfig.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);

    traceStoreTopology = new TraceStoreStream(traceSpansTopic.name, traceStoreName).get();

    // Store Stream Topology configuration
    traceAggregationStreamConfig = new Properties();
    traceAggregationStreamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
        builder.bootstrapServers);
    traceAggregationStreamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.StringSerde.class);
    traceAggregationStreamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.ByteArraySerde.class);
    traceAggregationStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,
        builder.traceAggregationStreamApplicationId);
    traceAggregationStreamConfig.put(StreamsConfig.STATE_DIR_CONFIG,
        builder.traceAggregationStreamStoreDirectory());
    traceAggregationStreamConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
        builder.compressionType.name);
    traceAggregationStreamConfig.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);

    traceInactivityGap = builder.traceInactivityGap;

    traceAggregationTopology = new TraceAggregationStream(
        traceSpansTopic.name, traceStoreName, tracesTopic.name, dependenciesTopic.name,
        traceInactivityGap).get();

    // Index Stream Topology configuration
    spanIndexStreamConfig = new Properties();
    spanIndexStreamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, builder.bootstrapServers);
    spanIndexStreamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.StringSerde.class);
    spanIndexStreamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.ByteArraySerde.class);
    spanIndexStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,
        builder.spanIndexStreamApplicationId);
    spanIndexStreamConfig.put(StreamsConfig.STATE_DIR_CONFIG,
        builder.spanIndexStreamStoreDirectory());
    spanIndexStreamConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
        builder.compressionType.name);
    spanIndexStreamConfig.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);

    spanIndexTopology = new SpanIndexStream(
        spansTopic.name, spanIndexStoreName, builder.indexStorageDirectory()).get();

    // Service stream configuration
    serviceStoreStreamConfig = new Properties();
    serviceStoreStreamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, builder.bootstrapServers);
    serviceStoreStreamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.StringSerde.class);
    serviceStoreStreamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.ByteArraySerde.class);
    serviceStoreStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,
        builder.serviceStoreStreamApplicationId);
    serviceStoreStreamConfig.put(StreamsConfig.STATE_DIR_CONFIG,
        builder.serviceStoreStreamStoreDirectory());
    serviceStoreStreamConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
        builder.compressionType.name);
    serviceStoreStreamConfig.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);

    serviceStoreTopology = new ServiceStoreStream(traceSpansTopic.name, serviceStoreName).get();

    // Dependency aggregation topology configuration
    dependencyStoreStreamConfig = new Properties();
    dependencyStoreStreamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
        builder.bootstrapServers);
    dependencyStoreStreamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.StringSerde.class);
    dependencyStoreStreamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.ByteArraySerde.class);
    dependencyStoreStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,
        builder.dependencyStoreStreamApplicationId);
    dependencyStoreStreamConfig.put(StreamsConfig.STATE_DIR_CONFIG,
        builder.serviceStoreStreamStoreDirectory());
    dependencyStoreStreamConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
        builder.compressionType.name);
    dependencyStoreStreamConfig.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);

    dependencyStoreTopology =
        new DependencyStoreStream(dependenciesTopic.name, dependencyStoreName).get();

    // Retention Stream Topology configuration
    traceRetentionStoreStreamConfig = new Properties();
    traceRetentionStoreStreamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
        builder.bootstrapServers);
    traceRetentionStoreStreamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.StringSerde.class);
    traceRetentionStoreStreamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.ByteArraySerde.class);
    traceRetentionStoreStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,
        builder.traceRetentionStoreStreamApplicationId);
    traceRetentionStoreStreamConfig.put(StreamsConfig.STATE_DIR_CONFIG,
        builder.retentionStreamStoreDirectory());
    traceAggregationStreamConfig.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);

    traceRetentionStoreTopology = new TraceRetentionStoreStream(
        traceSpansTopic.name, traceStoreName, builder.retentionScanFrequency,
        builder.retentionMaxAge).get();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public SpanConsumer spanConsumer() {
    if (spanConsumerEnabled) {
      return new KafkaSpanConsumer(this);
    } else { // NoopSpanConsumer
      return list -> Call.create(null);
    }
  }

  @Override
  public SpanStore spanStore() {
    if (spanStoreEnabled) {
      return new KafkaSpanStore(this);
    } else { // NoopSpanStore
      return new SpanStore() {
        @Override public Call<List<List<Span>>> getTraces(QueryRequest queryRequest) {
          return Call.emptyList();
        }

        @Override public Call<List<Span>> getTrace(String s) {
          return Call.emptyList();
        }

        @Override public Call<List<String>> getServiceNames() {
          return Call.emptyList();
        }

        @Override public Call<List<String>> getSpanNames(String s) {
          return Call.emptyList();
        }

        @Override public Call<List<DependencyLink>> getDependencies(long l, long l1) {
          return Call.emptyList();
        }
      };
    }
  }

  void ensureTopics() {
    try {
      Set<String> topics = getAdminClient().listTopics().names().get(1, TimeUnit.SECONDS);
      List<Topic> requiredTopics =
          Arrays.asList(spansTopic, traceSpansTopic, tracesTopic, dependenciesTopic);
      Set<NewTopic> newTopics = new HashSet<>();

      for (Topic requiredTopic : requiredTopics) {
        if (!topics.contains(requiredTopic.name)) {
          NewTopic newTopic = requiredTopic.newTopic();
          newTopics.add(newTopic);
        } else {
          LOG.info("Topic {} already exists.", requiredTopic.name);
        }
      }

      getAdminClient().createTopics(newTopics).all().get();
    } catch (Exception e) {
      LOG.error("Error ensuring topics are created", e);
    }
  }

  @Override public CheckResult check() {
    try {
      KafkaFuture<String> maybeClusterId = getAdminClient().describeCluster().clusterId();
      maybeClusterId.get(1, TimeUnit.SECONDS);
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
      if (adminClient != null) adminClient.close(1, TimeUnit.SECONDS);
      if (producer != null) {
        producer.flush();
        producer.close(1, TimeUnit.SECONDS);
      }
      if (spanConsumerStream != null) {
        spanConsumerStream.close(Duration.ofSeconds(1));
      }
      if (traceStoreStream != null) {
        traceStoreStream.close(Duration.ofSeconds(1));
      }
      if (traceAggregationStream != null) {
        traceAggregationStream.close(Duration.ofSeconds(1));
      }
      if (serviceStoreStream != null) {
        serviceStoreStream.close(Duration.ofSeconds(1));
      }
      if (spanIndexStream != null) {
        spanIndexStream.close(Duration.ofSeconds(1));
      }
      if (dependencyStoreStream != null) {
        dependencyStoreStream.close(Duration.ofSeconds(1));
      }
    } catch (Exception | Error e) {
      LOG.warn("error closing client {}", e.getMessage(), e);
    }
  }

  Producer<String, byte[]> getProducer() {
    if (producer == null) {
      synchronized (this) {
        if (producer == null) {
          if (ensureTopics) {
            ensureTopics();
          } else {
            LOG.info("Skipping topics creation as ensureTopics was false");
          }
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

  KafkaStreams getSpanConsumerStream() {
    if (spanConsumerStream == null) {
      synchronized (this) {
        if (spanConsumerStream == null) {
          spanConsumerStream = new KafkaStreams(spanConsumerTopology, spanConsumerStreamConfig);
          spanConsumerStream.start();
        }
      }
    }
    return spanConsumerStream;
  }

  KafkaStreams getSpanIndexStream() {
    if (spanIndexStream == null) {
      synchronized (this) {
        if (spanIndexStream == null) {
          try {
            spanIndexStream = new KafkaStreams(spanIndexTopology, spanIndexStreamConfig);
            spanIndexStream.start();
          }catch (Exception e) {
            LOG.error("Error building span index stream process", e);
            spanIndexStream = null;
          }
        }
      }
    }
    return spanIndexStream;
  }

  KafkaStreams getTraceStoreStream() {
    if (traceStoreStream == null) {
      synchronized (this) {
        if (traceStoreStream == null) {
          traceStoreStream = new KafkaStreams(traceStoreTopology, traceStoreStreamConfig);
          traceStoreStream.start();
        }
      }
    }
    return traceStoreStream;
  }

  KafkaStreams getTraceAggregationStream() {
    if (traceAggregationStream == null) {
      synchronized (this) {
        if (traceAggregationStream == null) {
          traceAggregationStream =
              new KafkaStreams(traceAggregationTopology, traceAggregationStreamConfig);
          traceAggregationStream.start();
        }
      }
    }
    return traceAggregationStream;
  }

  KafkaStreams getServiceStoreStream() {
    if (serviceStoreStream == null) {
      synchronized (this) {
        if (serviceStoreStream == null) {
          serviceStoreStream = new KafkaStreams(serviceStoreTopology, serviceStoreStreamConfig);
          serviceStoreStream.start();
        }
      }
    }
    return serviceStoreStream;
  }

  KafkaStreams getDependencyStoreStream() {
    if (dependencyStoreStream == null) {
      synchronized (this) {
        if (dependencyStoreStream == null) {
          dependencyStoreStream =
              new KafkaStreams(dependencyStoreTopology, dependencyStoreStreamConfig);
          dependencyStoreStream.start();
        }
      }
    }
    return dependencyStoreStream;
  }

  KafkaStreams getTraceRetentionStream() {
    if (traceRetentionStoreStream == null) {
      synchronized (this) {
        if (traceRetentionStoreStream == null) {
          traceRetentionStoreStream =
              new KafkaStreams(traceRetentionStoreTopology, traceRetentionStoreStreamConfig);
          traceRetentionStoreStream.start();
        }
      }
    }
    return traceRetentionStoreStream;
  }

  public static class Builder extends StorageComponent.Builder {
    boolean spanConsumerEnabled = true;
    boolean spanStoreEnabled = true;

    Duration retentionScanFrequency = Duration.ofMinutes(1);
    Duration retentionMaxAge = Duration.ofMinutes(2);

    String bootstrapServers = "localhost:29092";
    CompressionType compressionType = CompressionType.NONE;

    Duration traceInactivityGap = Duration.ofMinutes(5);

    String spanConsumerStreamApplicationId = "zipkin-server-span-consumer_v1";
    String traceStoreStreamApplicationId = "zipkin-server-trace-store_v1";
    String traceAggregationStreamApplicationId = "zipkin-server-trace-aggregation_v1";
    String serviceStoreStreamApplicationId = "zipkin-server-service-store_v1";
    String dependencyStoreStreamApplicationId = "zipkin-server-dependency-store_v1";
    String spanIndexStreamApplicationId = "zipkin-server-span-index_v1";
    String traceRetentionStoreStreamApplicationId = "zipkin-server-trace-retention_v1";

    String storeDirectory = "/tmp/zipkin";
    String spanIndexStoreName = "zipkin-index-store_v1";
    String traceStoreName = "zipkin-trace-store_v1";
    String dependencyStoreName = "zipkin-dependency_v1";
    String serviceStoreName = "zipkin-service_v1";

    Topic spansTopic = Topic.builder("zipkin-spans_v1")
        .build();
    Topic traceSpansTopic = Topic.builder("zipkin-trace-spans_v1")
        .build();
    Topic tracesTopic = Topic.builder("zipkin-traces_v1")
        .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
        .build();
    Topic dependenciesTopic = Topic.builder("zipkin-dependencies_v1")
        .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
        .build();

    boolean ensureTopics = true;

    Builder() {
    }

    @Override
    public StorageComponent.Builder strictTraceId(boolean strictTraceId) {
      if (!strictTraceId) throw new IllegalArgumentException("unstrict trace ID not supported");
      return this;
    }

    @Override
    public StorageComponent.Builder searchEnabled(boolean searchEnabled) {
      if (searchEnabled) throw new IllegalArgumentException("search not supported");
      return this;
    }

    @Override
    public Builder autocompleteKeys(List<String> keys) {
      if (keys == null) throw new NullPointerException("keys == null");
      if (!keys.isEmpty()) throw new IllegalArgumentException("autocomplete not supported");
      return this;
    }

    /**
     * Enable consuming spans from collectors and store them in Kafka topics.
     *
     * When disabled, a NoopSpanConsumer is instantiated to do nothing with incoming spans.
     */
    public Builder spanConsumerEnabled(boolean spanConsumerEnabled) {
      this.spanConsumerEnabled = spanConsumerEnabled;
      return this;
    }

    /**
     * Enable storing spans to aggregate and index spans, traces, and dependencies.
     *
     * When disabled, a NoopSpanStore is instantiated to return empty lists for all searches.
     */
    public Builder spanStoreEnabled(boolean spanStoreEnabled) {
      this.spanConsumerEnabled = spanStoreEnabled;
      return this;
    }

    public Builder traceInactivityGap(Duration traceInactivityGap) {
      if (traceInactivityGap == null) throw new NullPointerException("traceInactivityGap == null");
      this.traceInactivityGap = traceInactivityGap;
      return this;
    }

    /**
     * Kafka Bootstrap Servers list to establish connection with a Cluster.
     */
    public Builder bootstrapServers(String bootstrapServers) {
      if (bootstrapServers == null) throw new NullPointerException("bootstrapServers == null");
      this.bootstrapServers = bootstrapServers;
      return this;
    }

    /**
     * Kafka topic name where incoming raw spans are stored.
     *
     * A Raw span is a span received from Collectors that contains all metadata and is partitioned
     * by Span Id.
     */
    public Builder spansTopic(Topic spansTopic) {
      if (spansTopic == null) throw new NullPointerException("spansTopic == null");
      this.spansTopic = spansTopic;
      return this;
    }

    /**
     * Kafka topic name where "light" Spans, partitioned by Trace Id are stored.
     *
     * A Light Spans is a span without annotations and tags.
     */
    public Builder traceSpansTopic(Topic traceSpansTopic) {
      if (traceSpansTopic == null) {
        throw new NullPointerException("dependenciesTopic == null");
      }
      this.traceSpansTopic = traceSpansTopic;
      return this;
    }

    /**
     * Kafka topic name where aggregated traces changelog are stored. This topic is meant to be
     * compacted.
     *
     * This Trace changelog represents how traces grow on time.
     */
    public Builder tracesTopic(Topic tracesTopic) {
      if (tracesTopic == null) throw new NullPointerException("tracesTopic == null");
      this.tracesTopic = tracesTopic;
      return this;
    }

    /**
     * Kafka topic name where dependencies changelog are stored.
     */
    public Builder dependenciesTopic(Topic dependenciesTopic) {
      if (dependenciesTopic == null) {
        throw new NullPointerException("dependenciesTopic == null");
      }
      this.dependenciesTopic = dependenciesTopic;
      return this;
    }

    /**
     * Path to root directory when aggregated and indexed data is stored.
     */
    public Builder storeDirectory(String storeDirectory) {
      if (storeDirectory == null) {
        throw new NullPointerException("storageDirectory == null");
      }
      this.storeDirectory = storeDirectory;
      return this;
    }

    /**
     * Frequency to check retention policy.
     */
    public Builder retentionScanFrequency(Duration retentionScanFrequency) {
      this.retentionScanFrequency = retentionScanFrequency;
      return this;
    }

    /**
     * Maximum age for traces and spans to be retained on State Stores.
     */
    public Builder retentionMaxAge(Duration retentionMaxAge) {
      this.retentionMaxAge = retentionMaxAge;
      return this;
    }

    /**
     * If enabled, will create Topics if they do not exist.
     */
    public Builder ensureTopics(boolean ensureTopics) {
      this.ensureTopics = ensureTopics;
      return this;
    }

    public Builder compressionType(String compressionType) {
      if (compressionType == null) throw new NullPointerException("compressionType == null");
      this.compressionType = CompressionType.valueOf(compressionType);
      return this;
    }

    String spanConsumerStreamStoreDirectory() {
      return storeDirectory + "/streams/spans";
    }

    String traceStoreStreamStoreDirectory() {
      return storeDirectory + "/streams/traces";
    }

    String traceAggregationStreamStoreDirectory() {
      return storeDirectory + "/streams/trace-aggregation";
    }

    String serviceStoreStreamStoreDirectory() {
      return storeDirectory + "/streams/services";
    }

    String retentionStreamStoreDirectory() {
      return storeDirectory + "/streams/retention";
    }

    String spanIndexStreamStoreDirectory() {
      return storeDirectory + "/streams/span-index";
    }

    String indexStorageDirectory() {
      return storeDirectory + "/index";
    }

    @Override
    public StorageComponent build() {
      return new KafkaStorage(this);
    }
  }

  public static class Topic {
    final String name;
    final Integer partitions;
    final Short replicationFactor;
    final Map<String, String> configs;

    Topic(Builder builder) {
      this.name = builder.name;
      this.partitions = builder.partitions;
      this.replicationFactor = builder.replicationFactor;
      this.configs = builder.configs;
    }

    NewTopic newTopic() {
      NewTopic newTopic = new NewTopic(name, partitions, replicationFactor);
      newTopic.configs(configs);
      return newTopic;
    }

    public static Builder builder(String name) {
      return new Builder(name);
    }

    public static class Builder {
      final String name;
      Integer partitions = 1;
      Short replicationFactor = 1;
      Map<String, String> configs = new HashMap<>();

      Builder(String name) {
        if (name == null) throw new NullPointerException("topic name == null");
        this.name = name;
      }

      public Builder partitions(Integer partitions) {
        if (partitions == null) throw new NullPointerException("topic partitions == null");
        if (partitions < 1) throw new IllegalArgumentException("topic partitions < 1");
        this.partitions = partitions;
        return this;
      }

      public Builder replicationFactor(Short replicationFactor) {
        if (replicationFactor == null) {
          throw new NullPointerException("topic replicationFactor == null");
        }
        if (replicationFactor < 1) {
          throw new IllegalArgumentException("topic replicationFactor < 1");
        }
        this.replicationFactor = replicationFactor;
        return this;
      }

      Builder config(String key, String value) {
        if (key == null) throw new NullPointerException("topic config key == null");
        if (value == null) throw new NullPointerException("topic config value == null");
        this.configs.put(key, value);
        return this;
      }

      public Topic build() {
        return new Topic(this);
      }
    }
  }
}
