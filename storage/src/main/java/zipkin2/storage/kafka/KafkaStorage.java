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
import zipkin2.storage.kafka.streams.aggregation.DependencyLinkMapperSupplier;
import zipkin2.storage.kafka.streams.aggregation.TraceAggregationSupplier;
import zipkin2.storage.kafka.streams.stores.DependencyStoreSupplier;
import zipkin2.storage.kafka.streams.stores.TraceStoreSupplier;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Kafka Storage entry-point.
 *
 * Storage implementation based on Kafka Streams State Stores, supporting aggregation of spans,
 * indexing of traces and retention management.
 */
public class KafkaStorage extends StorageComponent {
  static final Logger LOG = LoggerFactory.getLogger(KafkaStorage.class);

  // Kafka Storage modes
  final boolean spanConsumerEnabled, spanStoreEnabled, aggregationEnabled;
  final boolean ensureTopics;
  // Kafka Storage configs
  final String storageDirectory;
  // Kafka Topics
  final Topic spansTopic, tracesTopic, dependencyLinksTopic;
  // Kafka Clients config
  final Properties adminConfig;
  final Properties producerConfig;
  // Kafka Streams topology configs
  final Properties
          traceAggregationStreamConfig, traceStoreStreamConfig,
          dependencyStoreStreamConfig, dependencyLinkMapperStreamConfig;
  final Topology
          traceAggregationTopology, traceStoreTopology, dependencyLinkMapperTopology, dependencyStoreTopology;
  // Resources
  volatile AdminClient adminClient;
  volatile Producer<String, byte[]> producer;
  volatile KafkaStreams dependencyLinkMapperStream,
          dependencyStoreStream, traceAggregationStream, traceStoreStream;
  volatile boolean closeCalled, topicsValidated;

  KafkaStorage(Builder builder) {
    // Kafka Storage modes
    this.spanConsumerEnabled = builder.spanConsumerEnabled;
    this.spanStoreEnabled = builder.spanStoreEnabled;
    this.aggregationEnabled = builder.aggregationEnabled;
    // Kafka Topics config
    this.ensureTopics = builder.ensureTopics;
    this.spansTopic = builder.spansTopic;
    this.tracesTopic = builder.tracesTopic;
    this.dependencyLinksTopic = builder.dependenciesTopic;
    // State store directories
    this.storageDirectory = builder.storeDirectory;
    // Kafka Clients configuration
    adminConfig = new Properties();
    adminConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, builder.bootstrapServers);
    // Kafka Producer configuration
    producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, builder.bootstrapServers);
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    producerConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, builder.compressionType.name);
    // Trace Aggregation topology
    traceAggregationStreamConfig = new Properties();
    traceAggregationStreamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
            builder.bootstrapServers);
    traceAggregationStreamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
            Serdes.StringSerde.class);
    traceAggregationStreamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
            Serdes.ByteArraySerde.class);
    traceAggregationStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,
            builder.traceAggregationStreamAppId);
    traceAggregationStreamConfig.put(StreamsConfig.STATE_DIR_CONFIG, builder.traceStoreDirectory());
    traceAggregationStreamConfig.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
    traceAggregationStreamConfig.put(
            StreamsConfig.PRODUCER_PREFIX + ProducerConfig.COMPRESSION_TYPE_CONFIG,
            builder.compressionType.name);
    traceAggregationTopology = new TraceAggregationSupplier(spansTopic.name, tracesTopic.name, builder.traceInactivityGap).get();
    // Trace Store Stream Topology configuration
    traceStoreStreamConfig = new Properties();
    traceStoreStreamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, builder.bootstrapServers);
    traceStoreStreamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.StringSerde.class);
    traceStoreStreamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.ByteArraySerde.class);
    traceStoreStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,
        builder.traceStoreStreamAppId);
    traceStoreStreamConfig.put(StreamsConfig.STATE_DIR_CONFIG,
        builder.traceStoreDirectory());
    traceStoreStreamConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
        builder.compressionType.name);
    traceStoreStreamConfig.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
    traceStoreTopology = new TraceStoreSupplier(spansTopic.name, builder.retentionScanFrequency, builder.retentionMaxAge).get();
    // Dependency Aggregation topology
    dependencyLinkMapperStreamConfig = new Properties();
    dependencyLinkMapperStreamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
        builder.bootstrapServers);
    dependencyLinkMapperStreamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.StringSerde.class);
    dependencyLinkMapperStreamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.ByteArraySerde.class);
    dependencyLinkMapperStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,
        builder.dependencyAggregationStreamAppId);
    dependencyLinkMapperStreamConfig.put(StreamsConfig.STATE_DIR_CONFIG,
        builder.dependencyStoreDirectory());
    dependencyLinkMapperStreamConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
        builder.compressionType.name);
    dependencyLinkMapperStreamConfig.put(StreamsConfig.TOPOLOGY_OPTIMIZATION,
        StreamsConfig.OPTIMIZE);
    dependencyLinkMapperTopology = new DependencyLinkMapperSupplier(tracesTopic.name, dependencyLinksTopic.name).get();
    // Dependency Store topology
    dependencyStoreStreamConfig = new Properties();
    dependencyStoreStreamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
        builder.bootstrapServers);
    dependencyStoreStreamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.StringSerde.class);
    dependencyStoreStreamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.ByteArraySerde.class);
    dependencyStoreStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,
        builder.dependencyStoreStreamAppId);
    dependencyStoreStreamConfig.put(StreamsConfig.STATE_DIR_CONFIG,
        builder.dependencyStoreDirectory());
    dependencyStoreStreamConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
        builder.compressionType.name);
    dependencyStoreStreamConfig.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
    dependencyStoreTopology = new DependencyStoreSupplier(dependencyLinksTopic.name, builder.retentionScanFrequency, builder.retentionMaxAge).get();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public SpanConsumer spanConsumer() {
    if (ensureTopics && !topicsValidated) ensureTopics();
    if (aggregationEnabled) {
      getTraceAggregationStream();

//      getServiceAggregationStream();
      //FIXME getDependencyLinkMapperStream();
    }
    if (spanConsumerEnabled) {
      return new KafkaSpanConsumer(this);
    } else { // NoopSpanConsumer
      return list -> Call.create(null);
    }
  }

  @Override
  public SpanStore spanStore() {
    if (ensureTopics && !topicsValidated) ensureTopics();
    if (aggregationEnabled) {
      getTraceAggregationStream();
      getDependencyLinkMapperStream();
    }
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
    if (!topicsValidated) {
      synchronized (this) {
        if (!topicsValidated) {
          try {
            Set<String> topics = getAdminClient().listTopics().names().get(1, TimeUnit.SECONDS);
            List<Topic> requiredTopics =
                    Arrays.asList(spansTopic, dependencyLinksTopic, tracesTopic);
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
        producer.close(Duration.ofSeconds(1));
      }
      if (traceStoreStream != null) {
        traceStoreStream.close(Duration.ofSeconds(1));
      }
      if (traceStoreStream != null) {
        traceStoreStream.close(Duration.ofSeconds(1));
      }
      if (traceAggregationStream != null) {
        traceAggregationStream.close(Duration.ofSeconds(1));
      }
      if (dependencyLinkMapperStream != null) {
        dependencyLinkMapperStream.close(Duration.ofSeconds(1));
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
          traceStoreStream = new KafkaStreams(traceStoreTopology, traceStoreStreamConfig);
          traceStoreStream.start();
          //getTraceRetentionStream();
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

  KafkaStreams getDependencyLinkMapperStream() {
    if (dependencyLinkMapperStream == null) {
      synchronized (this) {
        if (dependencyLinkMapperStream == null) {
          dependencyLinkMapperStream =
              new KafkaStreams(dependencyLinkMapperTopology, dependencyLinkMapperStreamConfig);
          dependencyLinkMapperStream.start();
        }
      }
    }
    return dependencyLinkMapperStream;
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

  public static class Builder extends StorageComponent.Builder {
    boolean spanConsumerEnabled = true;
    boolean spanStoreEnabled = true;
    boolean aggregationEnabled = true;

    Duration retentionScanFrequency = Duration.ofMinutes(1);
    Duration retentionMaxAge = Duration.ofMinutes(2);

    String bootstrapServers = "localhost:29092";
    CompressionType compressionType = CompressionType.NONE;

    Duration traceInactivityGap = Duration.ofMinutes(1);

    String traceStoreStreamAppId = "zipkin-trace-store-v1";
    String traceAggregationStreamAppId = "zipkin-trace-aggregation-v1";
    String dependencyStoreStreamAppId = "zipkin-dependency-store-v1";
    String dependencyAggregationStreamAppId = "zipkin-dependency-aggregation-v1";

    String storeDirectory = "/tmp/zipkin";

    Topic spansTopic = Topic.builder("zipkin-spans-v1").build();
    Topic spanServicesTopic = Topic.builder("zipkin-span-services-v1").build();
    Topic spanDependenciesTopic = Topic.builder("zipkin-span-dependencies-v1").build();
    Topic tracesTopic = Topic.builder("zipkin-traces-v1")
        .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
        .build();
    Topic servicesTopic = Topic.builder("zipkin-services-v1")
        .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
        .build();
    Topic dependenciesTopic = Topic.builder("zipkin-dependencies-v1")
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
     * Kafka topic name where incoming spans are stored.
     *
     * A Span is received from Collectors that contains all metadata and is partitioned
     * by Trace Id.
     */
    public Builder spansTopic(Topic spansTopic) {
      if (spansTopic == null) throw new NullPointerException("spansTopic == null");
      this.spansTopic = spansTopic;
      return this;
    }

    /**
     * Kafka topic name where span services events are stored.
     */
    public Builder spanServicesTopic(Topic spanServicesTopic) {
      if (spanServicesTopic == null) throw new NullPointerException("spanServicesTopic == null");
      this.spanServicesTopic = spanServicesTopic;
      return this;
    }

    /**
     * Kafka topic name where services changelog are stored.
     */
    public Builder servicesTopic(Topic servicesTopic) {
      if (servicesTopic == null) throw new NullPointerException("servicesTopic == null");
      this.servicesTopic = servicesTopic;
      return this;
    }

    /**
     * Kafka topic name where span dependencies events are stored.
     */
    public Builder spanDependenciesTopic(Topic spanDependenciesTopic) {
      if (spanDependenciesTopic == null) {
        throw new NullPointerException("spanDependenciesTopic == null");
      }
      this.spanDependenciesTopic = spanDependenciesTopic;
      return this;
    }

    /**
     * Kafka topic name where dependencies changelog are stored.
     */
    public Builder dependenciesTopic(Topic dependenciesTopic) {
      if (dependenciesTopic == null) throw new NullPointerException("dependencyLinksTopic == null");
      this.dependenciesTopic = dependenciesTopic;
      return this;
    }

    /**
     * Path to root directory when aggregated and indexed data is stored.
     */
    public Builder storeDirectory(String storeDirectory) {
      if (storeDirectory == null) throw new NullPointerException("storageDirectory == null");
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

    String traceStoreDirectory() {
      return storeDirectory + "/streams/traces";
    }

    String dependencyStoreDirectory() {
      return storeDirectory + "/streams/dependencies";
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
