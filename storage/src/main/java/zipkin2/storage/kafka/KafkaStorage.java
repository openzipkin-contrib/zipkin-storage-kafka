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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
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
import zipkin2.CheckResult;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StorageComponent;
import zipkin2.storage.kafka.internal.IndexTopologySupplier;
import zipkin2.storage.kafka.internal.ProcessTopologySupplier;

public class KafkaStorage extends StorageComponent {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaStorage.class);

  final boolean ensureTopics;
  final String storeDirectory;
  final Properties adminConfigs;
  final Properties producerConfigs;
  final Properties processStreamsConfig;
  final Properties indexStreamsConfig;

  final Topology processTopology;
  final Topology indexTopology;

  final Topic spansTopic;
  final Topic tracesTopic;
  final Topic servicesTopic;
  final Topic dependenciesTopic;

  final String indexStoreName;
  final boolean indexPersistent;

  volatile AdminClient adminClient;
  Producer<String, byte[]> producer;
  KafkaStreams processStreams;
  KafkaStreams indexStreams;

  volatile boolean closeCalled, connected;

  KafkaStorage(Builder builder) {
    this.ensureTopics = builder.ensureTopics;
    this.storeDirectory = builder.storeDirectory;
    this.tracesTopic = builder.tracesTopic;
    this.servicesTopic = builder.servicesTopic;
    this.dependenciesTopic = builder.dependenciesTopic;
    this.indexStoreName = builder.indexStoreName;
    this.spansTopic = builder.spansTopic;
    this.indexPersistent = builder.indexPersistent;

    adminConfigs = new Properties();
    adminConfigs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, builder.bootstrapServers);

    producerConfigs = new Properties();
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, builder.bootstrapServers);
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    producerConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    producerConfigs.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, builder.compressionType.name);

    processStreamsConfig = new Properties();
    processStreamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, builder.bootstrapServers);
    processStreamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.StringSerde.class);
    processStreamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.ByteArraySerde.class);
    processStreamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,
        builder.processStreamApplicationId);
    processStreamsConfig.put(StreamsConfig.EXACTLY_ONCE, true);
    processStreamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, builder.processStreamStoreDirectory());
    processStreamsConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, builder.compressionType.name);

    processTopology =
        new ProcessTopologySupplier(spansTopic.name, tracesTopic.name, servicesTopic.name,
            dependenciesTopic.name).get();

    indexStreamsConfig = new Properties();
    indexStreamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, builder.bootstrapServers);
    indexStreamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    indexStreamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.ByteArraySerde.class);
    indexStreamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, builder.indexStreamApplicationId);
    indexStreamsConfig.put(StreamsConfig.EXACTLY_ONCE, true);
    indexStreamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, builder.indexStreamStoreDirectory());

    indexTopology =
        new IndexTopologySupplier(tracesTopic.name, indexStoreName, builder.indexPersistent,
            builder.indexStorageDirectory()).get();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  void connect() {
    if (closeCalled) throw new IllegalStateException("closed");
    if (!connected) {
      // blocking to prevent access while initializing
      synchronized (this) {
        if (closeCalled) throw new IllegalStateException("closed");
        if (!connected) {
          doConnect();
        }
      }
    }
  }

  private void doConnect() {
    if (ensureTopics) {
      ensureTopics();
    } else {
      LOG.info("Skipping topics creation as ensureTopics was false");
    }
    connectStore();
    connectAdmin();
    connectConsumer();
    connected = true;
  }

  void connectAdmin() {
    adminClient = AdminClient.create(adminConfigs);
  }

  void connectConsumer() {
    producer = new KafkaProducer<>(producerConfigs);
  }

  void connectStore() {
    processStreams = new KafkaStreams(processTopology, processStreamsConfig);
    processStreams.start();

    indexStreams = new KafkaStreams(indexTopology, indexStreamsConfig);
    indexStreams.start();
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

  AdminClient getAdminClient() {
    if (adminClient == null) {
      synchronized (this) {
        if (adminClient == null) {
          adminClient = AdminClient.create(adminConfigs);
        }
      }
    }
    return adminClient;
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
  public SpanStore spanStore() {
    connect();
    return new KafkaSpanStore(this);
  }

  @Override
  public SpanConsumer spanConsumer() {
    connect();
    return new KafkaSpanConsumer(this);
  }

  @Override
  public void close() {
    if (closeCalled) return;
    // blocking to prevent access while initializing
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
      if (processStreams != null) processStreams.close(Duration.ofSeconds(1));
      if (indexStreams != null) indexStreams.close(Duration.ofSeconds(1));
    } catch (Exception | Error e) {
      LOG.warn("error closing client {}", e.getMessage(), e);
    }
  }

  public static class Builder extends StorageComponent.Builder {
    String bootstrapServers = "localhost:29092";
    CompressionType compressionType = CompressionType.NONE;
    String storeDirectory = "/tmp/zipkin";

    String processStreamApplicationId = "zipkin-server-process_v1";
    String indexStreamApplicationId = "zipkin-server-index_v1";
    String indexStoreName = "zipkin-index-store_v1";

    boolean indexPersistent = true;

    Topic spansTopic = Topic.builder("zipkin-spans_v1").build();
    Topic tracesTopic = Topic.builder("zipkin-traces_v1")
        .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
        .build();
    Topic servicesTopic = Topic.builder("zipkin-services_v1")
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
     * Store directory.
     */
    public Builder storeDirectory(String storeDirectory) {
      if (storeDirectory == null) {
        throw new NullPointerException("storeDirectory == null");
      }
      this.storeDirectory = storeDirectory;
      return this;
    }

    /**
     * Condition to use persistent index or not.
     */
    public Builder indexPersistent(boolean indexPersistent) {
      this.indexPersistent = indexPersistent;
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
      if (!Stream.of(CompressionType.values()).map(CompressionType::name).findAny().isPresent()) {
        throw new IllegalArgumentException("compressionType == invalid");
      }
      this.compressionType = CompressionType.valueOf(compressionType);
      return this;
    }

    String processStreamStoreDirectory() {
      return storeDirectory + "/kafka-streams/process";
    }

    String indexStreamStoreDirectory() {
      return storeDirectory + "/kafka-streams/index";
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
