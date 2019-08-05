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

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
import zipkin2.storage.kafka.index.SpanIndexService;
import zipkin2.storage.kafka.streams.DependencyAggregationStream;
import zipkin2.storage.kafka.streams.DependencyStoreStream;
import zipkin2.storage.kafka.streams.ServiceAggregationStream;
import zipkin2.storage.kafka.streams.ServiceStoreStream;
import zipkin2.storage.kafka.streams.TraceAggregationSupplier;
import zipkin2.storage.kafka.streams.TraceStoreSupplier;

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
  final String storageDirectory, traceStoreName, dependencyStoreName, serviceStoreName;
  // Kafka Topics
  final Topic spansTopic, tracesTopic, spanServicesTopic, servicesTopic, spanDependenciesTopic,
      dependenciesTopic;
  // Kafka Clients config
  final Properties adminConfig;
  final Properties producerConfig;
  // Kafka Streams topology configs
  final Properties traceStoreStreamConfig, serviceStoreStreamConfig, dependencyStoreStreamConfig,
      serviceAggregationStreamConfig, dependencyAggregationStreamConfig, traceRetentionStreamConfig,
      traceAggregationStreamConfig;
  final Topology traceStoreTopology, serviceStoreTopology, dependencyStoreTopology,
      serviceAggregationTopology, dependencyAggregationTopology, traceAggregationTopology;
      //traceRetentionTopology; //FIXME
  final String spanIndexDirectory;
  // Resources
  volatile AdminClient adminClient;
  volatile Producer<String, byte[]> producer;
  volatile KafkaStreams serviceAggregationStream, dependencyAggregationStream, traceStoreStream,
      serviceStoreStream, dependencyStoreStream, traceAggregationStream, traceRetentionStream;
  volatile boolean closeCalled, topicsValidated;
  volatile SpanIndexService spanIndexService;
  volatile Map<String, Set<String>> serviceSpanMap;

  KafkaStorage(Builder builder) {
    // Kafka Storage modes
    this.spanConsumerEnabled = builder.spanConsumerEnabled;
    this.spanStoreEnabled = builder.spanStoreEnabled;
    this.aggregationEnabled = builder.aggregationEnabled;
    // Kafka Topics config
    this.ensureTopics = builder.ensureTopics;
    this.spansTopic = builder.spansTopic;
    this.tracesTopic = builder.tracesTopic;
    this.spanServicesTopic = builder.spanServicesTopic;
    this.servicesTopic = builder.servicesTopic;
    this.spanDependenciesTopic = builder.spanDependenciesTopic;
    this.dependenciesTopic = builder.dependenciesTopic;
    // State store directories
    this.storageDirectory = builder.storeDirectory;
    this.traceStoreName = builder.traceStoreName;
    this.dependencyStoreName = builder.dependencyStoreName;
    this.serviceStoreName = builder.serviceStoreName;
    // Span Index service
    spanIndexDirectory = builder.spanIndexDirectory();
    // Service:Span names map
    serviceSpanMap = new ConcurrentHashMap<>();
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
    traceStoreTopology =
        // FIXME
        new TraceStoreSupplier(spansTopic.name, traceStoreName, "",
            builder.retentionScanFrequency, builder.retentionMaxAge).get();
    // Service Aggregation topology
    serviceAggregationStreamConfig = new Properties();
    serviceAggregationStreamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
        builder.bootstrapServers);
    serviceAggregationStreamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.StringSerde.class);
    serviceAggregationStreamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.ByteArraySerde.class);
    serviceAggregationStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,
        builder.serviceAggregationStreamAppId);
    serviceAggregationStreamConfig.put(StreamsConfig.STATE_DIR_CONFIG,
        builder.serviceStoreDirectory());
    serviceAggregationStreamConfig.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
    serviceAggregationTopology =
        new ServiceAggregationStream(spanServicesTopic.name, servicesTopic.name)
            .get();
    // Service Store topology
    serviceStoreStreamConfig = new Properties();
    serviceStoreStreamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, builder.bootstrapServers);
    serviceStoreStreamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.StringSerde.class);
    serviceStoreStreamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.ByteArraySerde.class);
    serviceStoreStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,
        builder.serviceStoreStreamAppId);
    serviceStoreStreamConfig.put(StreamsConfig.STATE_DIR_CONFIG, builder.serviceStoreDirectory());
    serviceStoreStreamConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
        builder.compressionType.name);
    serviceStoreStreamConfig.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
    serviceStoreTopology = new ServiceStoreStream(servicesTopic.name, serviceStoreName).get();
    // Dependency Aggregation topology
    dependencyAggregationStreamConfig = new Properties();
    dependencyAggregationStreamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
        builder.bootstrapServers);
    dependencyAggregationStreamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.StringSerde.class);
    dependencyAggregationStreamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.ByteArraySerde.class);
    dependencyAggregationStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,
        builder.dependencyAggregationStreamAppId);
    dependencyAggregationStreamConfig.put(StreamsConfig.STATE_DIR_CONFIG,
        builder.dependencyStoreDirectory());
    dependencyAggregationStreamConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
        builder.compressionType.name);
    dependencyAggregationStreamConfig.put(StreamsConfig.TOPOLOGY_OPTIMIZATION,
        StreamsConfig.OPTIMIZE);
    dependencyAggregationTopology =
        new DependencyAggregationStream(tracesTopic.name, spanDependenciesTopic.name,
            dependenciesTopic.name).get();
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
    dependencyStoreTopology =
        new DependencyStoreStream(dependenciesTopic.name, dependencyStoreName).get();
    // Trace Retention topology
    traceRetentionStreamConfig = new Properties();
    traceRetentionStreamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
        builder.bootstrapServers);
    traceRetentionStreamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.StringSerde.class);
    traceRetentionStreamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.ByteArraySerde.class);
    traceRetentionStreamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,
        builder.traceRetentionStreamAppId);
    traceRetentionStreamConfig.put(StreamsConfig.STATE_DIR_CONFIG, builder.traceStoreDirectory());
    traceRetentionStreamConfig.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
    traceRetentionStreamConfig.put(
        StreamsConfig.PRODUCER_PREFIX + ProducerConfig.COMPRESSION_TYPE_CONFIG,
        builder.compressionType.name);
    //traceRetentionTopology = new TraceRetentionStoreStream(spansTopic.name, traceStoreName,
    //    builder.retentionScanFrequency, builder.retentionMaxAge).get();
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
    traceAggregationTopology = new TraceAggregationSupplier(spansTopic.name, tracesTopic.name,
        builder.traceInactivityGap).get();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public SpanConsumer spanConsumer() {
    if (ensureTopics && !topicsValidated) ensureTopics();
    if (aggregationEnabled) {
      getTraceAggregationStream();
      getServiceAggregationStream();
      getDependencyAggregationStream();
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
      getServiceAggregationStream();
      getDependencyAggregationStream();
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
                Arrays.asList(spansTopic, spanServicesTopic, servicesTopic, spanDependenciesTopic,
                    dependenciesTopic, tracesTopic);
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
        producer.close(1, TimeUnit.SECONDS);
      }
      if (traceStoreStream != null) {
        traceStoreStream.close(Duration.ofSeconds(1));
      }
      if (traceRetentionStream != null) {
        traceRetentionStream.close(Duration.ofSeconds(1));
      }
      if (traceAggregationStream != null) {
        traceAggregationStream.close(Duration.ofSeconds(1));
      }
      if (serviceAggregationStream != null) {
        serviceAggregationStream.close(Duration.ofSeconds(1));
      }
      if (serviceStoreStream != null) {
        serviceStoreStream.close(Duration.ofSeconds(1));
      }
      if (dependencyAggregationStream != null) {
        dependencyAggregationStream.close(Duration.ofSeconds(1));
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

  // FIXME
  //KafkaStreams getTraceRetentionStream() {
  //  if (traceRetentionStream == null) {
  //    synchronized (this) {
  //      if (traceRetentionStream == null) {
  //        traceRetentionStream =
  //            new KafkaStreams(traceRetentionTopology, traceRetentionStreamConfig);
  //        traceRetentionStream.start();
  //      }
  //    }
  //  }
  //  return traceRetentionStream;
  //}

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

  KafkaStreams getServiceAggregationStream() {
    if (serviceAggregationStream == null) {
      synchronized (this) {
        if (serviceAggregationStream == null) {
          serviceAggregationStream =
              new KafkaStreams(serviceAggregationTopology, serviceAggregationStreamConfig);
          serviceAggregationStream.start();
        }
      }
    }
    return serviceAggregationStream;
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

  KafkaStreams getDependencyAggregationStream() {
    if (dependencyAggregationStream == null) {
      synchronized (this) {
        if (dependencyAggregationStream == null) {
          dependencyAggregationStream =
              new KafkaStreams(dependencyAggregationTopology, dependencyAggregationStreamConfig);
          dependencyAggregationStream.start();
        }
      }
    }
    return dependencyAggregationStream;
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

  SpanIndexService getSpanIndexService() {
    if (spanIndexService == null) {
      synchronized (this) {
        if (spanIndexService == null) {
          try {
            spanIndexService = SpanIndexService.create(spanIndexDirectory);
          } catch (IOException e) {
            LOG.error("Error creating span index service", e);
          }
        }
      }
    }
    return spanIndexService;
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
    String traceRetentionStreamAppId = "zipkin-trace-retention-v1";
    String serviceStoreStreamAppId = "zipkin-service-store-v1";
    String serviceAggregationStreamAppId = "zipkin-service-aggregation-v1";
    String dependencyStoreStreamAppId = "zipkin-dependency-store-v1";
    String dependencyAggregationStreamAppId = "zipkin-dependency-aggregation-v1";

    String storeDirectory = "/tmp/zipkin";

    String traceStoreName = "zipkin-trace-store-v1";
    String dependencyStoreName = "zipkin-dependency-v1";
    String serviceStoreName = "zipkin-service-v1";

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
      if (dependenciesTopic == null) throw new NullPointerException("dependenciesTopic == null");
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

    String serviceStoreDirectory() {
      return storeDirectory + "/streams/services";
    }

    String dependencyStoreDirectory() {
      return storeDirectory + "/streams/dependencies";
    }

    String spanIndexDirectory() {
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
