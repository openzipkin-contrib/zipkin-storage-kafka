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
import zipkin2.storage.kafka.streams.ServiceStoreStream;
import zipkin2.storage.kafka.streams.SpanConsumerStream;
import zipkin2.storage.kafka.streams.SpanIndexStream;
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

  // Kafka Storage configs
  final boolean ensureTopics;
  final String storeDirectory;

  // Kafka Topics
  final Topic spansTopic, traceSpansTopic, tracesTopic, servicesTopic, dependenciesTopic;

  // Kafka Clients config
  final Properties adminConfig;
  final Properties producerConfig;

  // Kafka Streams topology configs
  final Properties spanConsumerStreamConfig, traceStoreStreamConfig, spanIndexStreamConfig,
      serviceStoreStreamConfig, dependencyStoreStreamConfig;
  final Topology spanConsumerTopology, traceStoreTopology, spanIndexTopology, serviceStoreTopology,
      dependencyStoreTopology;
  final String indexStoreName;

  // Resources
  volatile AdminClient adminClient;
  volatile Producer<String, byte[]> producer;
  volatile KafkaStreams spanConsumerStream, traceStoreStream, spanIndexStream, serviceStoreStream,
      dependencyStoreStream;
  volatile boolean closeCalled;

  KafkaStorage(Builder builder) {
    this.spanConsumerEnabled = builder.spanConsumerEnabled;
    this.spanStoreEnabled = builder.spanStoreEnabled;

    this.ensureTopics = builder.ensureTopics;
    this.storeDirectory = builder.storeDirectory;
    this.tracesTopic = builder.tracesTopic;
    this.traceSpansTopic = builder.traceSpansTopic;
    this.servicesTopic = builder.servicesTopic;
    this.dependenciesTopic = builder.dependenciesTopic;
    this.indexStoreName = builder.indexStoreName;
    this.spansTopic = builder.spansTopic;

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
    spanConsumerStreamConfig.put(StreamsConfig.EXACTLY_ONCE, true);
    spanConsumerStreamConfig.put(StreamsConfig.STATE_DIR_CONFIG,
        builder.spanConsumerStreamStoreDirectory());
    spanConsumerStreamConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
        builder.compressionType.name);

    spanConsumerTopology =
        new SpanConsumerStream(spansTopic.name, servicesTopic.name, tracesTopic.name).get();

    // Store Stream Topology configuration
    traceStoreStreamConfig = new Properties();
    traceStoreStreamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, builder.bootstrapServers);
    traceStoreStreamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.StringSerde.class);
    traceStoreStreamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.ByteArraySerde.class);
    traceStoreStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,
        builder.traceStoreStreamApplicationId);
    traceStoreStreamConfig.put(StreamsConfig.EXACTLY_ONCE, true);
    traceStoreStreamConfig.put(StreamsConfig.STATE_DIR_CONFIG,
        builder.traceStoreStreamStoreDirectory());

    traceStoreTopology =
        new TraceStoreStream(spansTopic.name, traceSpansTopic.name, tracesTopic.name,
            tracesTopic.name, tracesTopic.name + "_global").get();

    // Index Stream Topology configuration
    spanIndexStreamConfig = new Properties();
    spanIndexStreamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, builder.bootstrapServers);
    spanIndexStreamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.StringSerde.class);
    spanIndexStreamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.ByteArraySerde.class);
    spanIndexStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,
        builder.spanIndexStreamApplicationId);
    spanIndexStreamConfig.put(StreamsConfig.EXACTLY_ONCE, true);
    spanIndexStreamConfig.put(StreamsConfig.STATE_DIR_CONFIG,
        builder.spanIndexStreamStoreDirectory());

    spanIndexTopology =
        new SpanIndexStream(spansTopic.name, tracesTopic.name,
            builder.indexStorageDirectory()).get();

    serviceStoreStreamConfig = new Properties();
    serviceStoreStreamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, builder.bootstrapServers);
    serviceStoreStreamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.StringSerde.class);
    serviceStoreStreamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.ByteArraySerde.class);
    serviceStoreStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,
        builder.serviceStoreStreamApplicationId);
    serviceStoreStreamConfig.put(StreamsConfig.EXACTLY_ONCE, true);
    serviceStoreStreamConfig.put(StreamsConfig.STATE_DIR_CONFIG,
        builder.serviceStoreStreamStoreDirectory());

    serviceStoreTopology = new ServiceStoreStream(servicesTopic.name, servicesTopic.name).get();

    dependencyStoreStreamConfig = new Properties();
    dependencyStoreStreamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
        builder.bootstrapServers);
    dependencyStoreStreamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.StringSerde.class);
    dependencyStoreStreamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.ByteArraySerde.class);
    dependencyStoreStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,
        builder.dependencyStoreStreamApplicationId);
    dependencyStoreStreamConfig.put(StreamsConfig.EXACTLY_ONCE, true);
    dependencyStoreStreamConfig.put(StreamsConfig.STATE_DIR_CONFIG,
        builder.serviceStoreStreamStoreDirectory());

    dependencyStoreTopology =
        new ServiceStoreStream(dependenciesTopic.name, dependenciesTopic.name).get();

    // Retention Stream Topology configuration
    //retentionStreamsConfig = new Properties();
    //retentionStreamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, builder.bootstrapServers);
    //retentionStreamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
    //    Serdes.StringSerde.class);
    //retentionStreamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
    //    Serdes.ByteArraySerde.class);
    //retentionStreamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,
    //    builder.retentionStreamApplicationId);
    //retentionStreamsConfig.put(StreamsConfig.EXACTLY_ONCE, true);
    //retentionStreamsConfig.put(StreamsConfig.STATE_DIR_CONFIG,
    //    builder.retentionStreamStoreDirectory());
    //
    //retentionTopology =
    //    new RetentionTopologySupplier(tracesTopic.name, builder.retentionScanFrequency,
    //        builder.retentionMaxAge).get();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public SpanConsumer spanConsumer() {
    if (spanConsumerEnabled) {
      return new KafkaSpanConsumer(this);
    } else {
      return list -> Call.create(null);
    }
  }

  @Override
  public SpanStore spanStore() {
    if (spanStoreEnabled) {
      return new KafkaSpanStore(this);
    } else {
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
          Arrays.asList(spansTopic, tracesTopic, servicesTopic, dependenciesTopic);
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

  @Override
  public CheckResult check() {
    try {
      KafkaFuture<String> maybeClusterId = getAdminClient().describeCluster().clusterId();
      maybeClusterId.get(1, TimeUnit.SECONDS);
      return CheckResult.OK;
    } catch (Exception e) {
      return CheckResult.failed(e);
    }
  }

  @Override
  public void close() {
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
          spanIndexStream = new KafkaStreams(spanIndexTopology, spanIndexStreamConfig);
          spanIndexStream.start();
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

  public static class Builder extends StorageComponent.Builder {
    boolean spanConsumerEnabled = true;
    boolean spanStoreEnabled = true;
    Duration retentionScanFrequency = Duration.ofMinutes(1);
    Duration retentionMaxAge = Duration.ofMinutes(2);
    String bootstrapServers = "localhost:29092";
    CompressionType compressionType = CompressionType.NONE;
    String storeDirectory = "/tmp/zipkin";

    String spanConsumerStreamApplicationId = "zipkin-server-span-consumer_v1";
    String traceStoreStreamApplicationId = "zipkin-server-trace-store_v1";
    String serviceStoreStreamApplicationId = "zipkin-server-service-store_v1";
    String dependencyStoreStreamApplicationId = "zipkin-server-dependency-store_v1";
    String spanIndexStreamApplicationId = "zipkin-server-span-index_v1";
    String retentionStreamApplicationId = "zipkin-server-retention_v1";

    String indexStoreName = "zipkin-index-store_v1";

    Topic spansTopic = Topic.builder("zipkin-spans_v1").build();
    Topic traceSpansTopic = Topic.builder("zipkin-trace-spans_v1")
        .build();
    Topic tracesTopic = Topic.builder("zipkin-traces_v1")
        .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
        .build();
    Topic servicesTopic = Topic.builder("zipkin-services_v1")
        .build();
    Topic dependenciesTopic = Topic.builder("zipkin-dependencies_v1")
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
     * Kafka Bootstrap Servers list to establish connection with a Cluster.
     */
    public Builder bootstrapServers(String bootstrapServers) {
      if (bootstrapServers == null) throw new NullPointerException("bootstrapServers == null");
      this.bootstrapServers = bootstrapServers;
      return this;
    }

    /**
     * Kafka topic name where incoming list of Spans are stored.
     */
    public Builder spansTopic(Topic spansTopic) {
      if (spansTopic == null) throw new NullPointerException("spansTopic == null");
      this.spansTopic = spansTopic;
      return this;
    }

    /**
     * Kafka topic name where traces are stored.
     */
    public Builder tracesTopic(Topic tracesTopic) {
      if (tracesTopic == null) throw new NullPointerException("tracesTopic == null");
      this.tracesTopic = tracesTopic;
      return this;
    }

    /**
     * Kafka topic name where Service names are stored.
     */
    public Builder servicesTopic(Topic servicesTopic) {
      if (servicesTopic == null) {
        throw new NullPointerException("servicesTopic == null");
      }
      this.servicesTopic = servicesTopic;
      return this;
    }

    /**
     * Kafka topic name where Dependencies are stored.
     */
    public Builder dependenciesTopic(Topic dependenciesTopic) {
      if (dependenciesTopic == null) {
        throw new NullPointerException("dependenciesTopic == null");
      }
      this.dependenciesTopic = dependenciesTopic;
      return this;
    }

    /**
     * Path to root directory when state is stored.
     */
    public Builder storeDirectory(String storeDirectory) {
      if (storeDirectory == null) {
        throw new NullPointerException("storeDirectory == null");
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
     * Frequency to check retention policy.
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
