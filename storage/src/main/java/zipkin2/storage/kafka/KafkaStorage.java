/*
 * Copyright 2019 [name of copyright owner]
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
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
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

  final Properties adminConfigs;
  final Properties producerConfigs;
  final Properties processStreamsConfig;
  final Properties indexStreamsConfig;
  final Topology processTopology;
  final Topology indexTopology;
  final String spansTopic;
  final String tracesTopic;
  final String servicesTopic;
  final String dependenciesTopic;
  final String indexStoreName;
  final boolean indexPersistent;

  AdminClient adminClient;
  Producer<String, byte[]> producer;
  KafkaStreamsWorker processStreamsWorker;
  KafkaStreams processStreams;
  KafkaStreamsWorker indexStreamsWorker;
  KafkaStreams indexStreams;

  volatile boolean closeCalled, connected;

  KafkaStorage(Builder builder) {
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
    producerConfigs.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, CompressionType.ZSTD.name);

    processStreamsConfig = new Properties();
    processStreamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, builder.bootstrapServers);
    processStreamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.StringSerde.class);
    processStreamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.ByteArraySerde.class);
    processStreamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,
        builder.processStreamApplicationId);
    processStreamsConfig.put(StreamsConfig.EXACTLY_ONCE, true);
    processStreamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, builder.processStreamStoreDirectory);
    processStreamsConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, CompressionType.ZSTD.name);

    processTopology = new ProcessTopologySupplier(spansTopic, tracesTopic, servicesTopic,
        dependenciesTopic).get();

    indexStreamsConfig = new Properties();
    indexStreamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, builder.bootstrapServers);
    indexStreamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    indexStreamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.ByteArraySerde.class);
    indexStreamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, builder.indexStreamApplicationId);
    indexStreamsConfig.put(StreamsConfig.EXACTLY_ONCE, true);
    indexStreamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, builder.indexStreamStoreDirectory);

    indexTopology =
        new IndexTopologySupplier(tracesTopic, indexStoreName, builder.indexDirectory).get();
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
          connectAdmin();
          connectConsumer();
          connectStore();
          connected = true;
        }
      }
    }
  }

  void connectAdmin() {
    adminClient = AdminClient.create(adminConfigs);
  }

  void connectConsumer() {
    producer = new KafkaProducer<>(producerConfigs);
  }

  void connectStore() {
    processStreams = new KafkaStreams(processTopology, processStreamsConfig);
    processStreamsWorker = new KafkaStreamsWorker(processStreams);
    processStreamsWorker.get();

    indexStreams = new KafkaStreams(indexTopology, indexStreamsConfig);
    indexStreamsWorker = new KafkaStreamsWorker(indexStreams);
    indexStreamsWorker.get();
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
      adminClient.close(1, TimeUnit.SECONDS);
      producer.flush();
      producer.close(1, TimeUnit.SECONDS);
      processStreams.close(Duration.ofSeconds(1));
      indexStreams.close(Duration.ofSeconds(1));
      processStreamsWorker.close();
      indexStreamsWorker.close();
    } catch (Exception | Error e) {
      LOG.warn("error closing client {}", e.getMessage(), e);
    }
  }

  public static class Builder extends StorageComponent.Builder {
    String bootstrapServers = "localhost:29092";
    String processStreamApplicationId = "zipkin-server-process_v1";
    String indexStreamApplicationId = "zipkin-server-index_v1";
    String indexStoreName = "zipkin-index-store_v1";

    String processStreamStoreDirectory = "/tmp/kafka-streams/process";
    String indexStreamStoreDirectory = "/tmp/kafka-streams/index";
    boolean indexPersistent = true;
    String indexDirectory = "/tmp/lucene-index";

    String spansTopic = "zipkin-spans_v1";
    String tracesTopic = "zipkin-traces_v1";
    String servicesTopic = "zipkin-services_v1";
    String dependenciesTopic = "zipkin-dependencies_v1";

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

    public Builder processStreamApplicationId(String processStreamApplicationId) {
      if (processStreamApplicationId == null) {
        throw new NullPointerException("processStreamApplicationId == null");
      }
      this.processStreamApplicationId = processStreamApplicationId;
      return this;
    }

    public Builder indexStreamApplicationId(String indexStreamApplicationId) {
      if (indexStreamApplicationId == null) {
        throw new NullPointerException("processStreamApplicationId == null");
      }
      this.indexStreamApplicationId = indexStreamApplicationId;
      return this;
    }

    /**
     * Kafka topic name where incoming list of Spans are stored.
     */
    public Builder spansTopic(String spansTopic) {
      if (spansTopic == null) throw new NullPointerException("spansTopic == null");
      this.spansTopic = spansTopic;
      return this;
    }

    /**
     * Kafka topic name where traces are stored.
     */
    public Builder tracesTopic(String tracesStoreName) {
      if (tracesStoreName == null) throw new NullPointerException("tracesTopic == null");
      this.tracesTopic = tracesStoreName;
      return this;
    }

    /**
     * Kafka topic name where Service names are stored.
     */
    public Builder servicesTopic(String serviceOperationsStoreName) {
      if (serviceOperationsStoreName == null) {
        throw new NullPointerException("servicesTopic == null");
      }
      this.servicesTopic = serviceOperationsStoreName;
      return this;
    }

    /**
     * Kafka topic name where Dependencies are stored.
     */
    public Builder dependenciesTopic(String dependenciesStoreName) {
      if (dependenciesStoreName == null) {
        throw new NullPointerException("dependenciesTopic == null");
      }
      this.dependenciesTopic = dependenciesStoreName;
      return this;
    }

    /**
     * Kafka Streams local state directory where processing results (e.g., traces, services,
     * dependencies) are stored.
     */
    public Builder processStreamStoreDirectory(String processStreamStoreDirectory) {
      if (processStreamStoreDirectory == null) {
        throw new NullPointerException("processStreamStoreDirectory == null");
      }
      this.processStreamStoreDirectory = processStreamStoreDirectory;
      return this;
    }

    /**
     * Directory where local state from index processing is stored.
     */
    public Builder indexStreamStoreDirectory(String indexStreamStoreDirectory) {
      if (indexStreamStoreDirectory == null) {
        throw new NullPointerException("processStreamStoreDirectory == null");
      }
      this.indexStreamStoreDirectory = indexStreamStoreDirectory;
      return this;
    }

    /**
     * Kafka Streams store name for indexing processing.
     */
    public Builder indexStoreName(String indexStoreName) {
      if (indexStoreName == null) throw new NullPointerException("indexStoreName == null");
      this.indexStoreName = indexStoreName;
      return this;
    }

    /**
     * Index storage directory.
     */
    public Builder indexDirectory(String indexDirectory) {
      if (indexDirectory == null) throw new NullPointerException("indexDirectory == null");
      this.indexDirectory = indexDirectory;
      return this;
    }

    /**
     * Condition to use persistent index or not.
     */
    public Builder indexPersistent(boolean indexPersistent) {
      this.indexPersistent = indexPersistent;
      return this;
    }

    @Override
    public StorageComponent build() {
      return new KafkaStorage(this);
    }
  }

  public static final class KafkaStreamsWorker {
    final KafkaStreams kafkaStreams;
    final AtomicReference<CheckResult> failure = new AtomicReference<>();
    volatile ExecutorService pool;

    KafkaStreamsWorker(KafkaStreams kafkaStreams) {
      this.kafkaStreams = kafkaStreams;
    }

    ExecutorService get() {
      if (pool == null) {
        synchronized (this) {
          if (pool == null) {
            pool = compute();
          }
        }
      }
      return pool;
    }

    void close() {
      ExecutorService maybePool = pool;
      if (maybePool == null) return;
      try {
        maybePool.awaitTermination(1, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOG.error("Error waiting for stream poll to close", e);
      }
    }

    private ExecutorService compute() {
      ExecutorService pool = Executors.newSingleThreadExecutor();
      pool.execute(guardFailures(kafkaStreams));
      return pool;
    }

    private Runnable guardFailures(KafkaStreams kafkaStreams) {
      return () -> {
        try {
          kafkaStreams.start();
        } catch (Exception e) {
          LOG.error("Kafka Streams worker exited with error", e);
          failure.set(CheckResult.failed(e));
        }
      };
    }
  }
}
