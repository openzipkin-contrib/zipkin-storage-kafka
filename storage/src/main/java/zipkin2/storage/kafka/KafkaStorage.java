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

import com.linecorp.armeria.server.Server;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.streams.KafkaStreams;
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
import zipkin2.storage.Traces;
import zipkin2.storage.kafka.streams.DependencyStorageTopology;
import zipkin2.storage.kafka.streams.SpanAggregationTopology;
import zipkin2.storage.kafka.streams.TraceStorageTopology;

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
  public static final String HTTP_PATH_PREFIX = "/storage/kafka";

  static final Logger LOG = LoggerFactory.getLogger(KafkaStorage.class);

  public static KafkaStorageBuilder newBuilder() {
    return new KafkaStorageBuilder();
  }

  // Kafka Storage flags
  final boolean partitioningEnabled;
  final boolean aggregationEnabled;
  final boolean traceByIdQueryEnabled;
  final boolean traceSearchEnabled;
  final boolean dependencyQueryEnabled;
  // Autocomplete Tags
  final List<String> autocompleteKeys;
  // Kafka Storage configs
  final long minTracesStored;
  final String hostname;
  final int httpPort;
  // Kafka Topics
  final String partitioningSpansTopic;
  final String aggregationSpansTopic, aggregationTraceTopic, aggregationDependencyTopic;
  final String storageSpansTopic, storageDependencyTopic;
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
  volatile KafkaStreams aggregationStream, traceStoreStream, dependencyStoreStream;
  volatile Server server;
  volatile boolean closeCalled;

  KafkaStorage(KafkaStorageBuilder builder) {
    // Kafka Storage modes
    this.partitioningEnabled = builder.spanPartitioning.enabled;
    this.aggregationEnabled = builder.spanAggregation.enabled;
    this.traceByIdQueryEnabled = builder.traceStorage.traceByIdQueryEnabled;
    this.traceSearchEnabled = builder.traceStorage.traceSearchEnabled;
    this.dependencyQueryEnabled = builder.dependencyStorage.enabled;
    // Autocomplete tags
    this.autocompleteKeys = builder.autocompleteKeys;
    // Kafka Topics config
    this.partitioningSpansTopic = builder.spanPartitioning.spansTopic;
    this.aggregationSpansTopic = builder.spanAggregation.spansTopic;
    this.aggregationTraceTopic = builder.spanAggregation.traceTopic;
    this.aggregationDependencyTopic = builder.spanAggregation.dependencyTopic;
    this.storageSpansTopic = builder.traceStorage.spansTopic;
    this.storageDependencyTopic = builder.dependencyStorage.dependencyTopic;
    // Storage directories
    this.minTracesStored = builder.traceStorage.minTracesStored;
    this.httpBaseUrl = builder.httpBaseUrl;
    this.hostname = builder.hostname;
    this.httpPort = builder.serverPort;
    // Kafka Configs
    this.adminConfig = builder.adminConfig;
    this.producerConfig = builder.spanPartitioning.producerConfig;
    this.aggregationStreamConfig = builder.spanAggregation.streamConfig;
    this.traceStoreStreamConfig = builder.traceStorage.streamConfig;
    this.dependencyStoreStreamConfig = builder.dependencyStorage.streamConfig;

    aggregationTopology = new SpanAggregationTopology(
        builder.spanAggregation.spansTopic,
        builder.spanAggregation.traceTopic,
        builder.spanAggregation.dependencyTopic,
        builder.spanAggregation.traceTimeout,
        builder.spanAggregation.enabled).get();
    traceStoreTopology = new TraceStorageTopology(
        builder.traceStorage.spansTopic,
        autocompleteKeys,
        builder.traceStorage.traceTtl,
        builder.traceStorage.traceTtlCheckInterval,
        builder.traceStorage.minTracesStored,
        builder.traceStorage.traceByIdQueryEnabled,
        builder.traceStorage.traceSearchEnabled).get();
    dependencyStoreTopology = new DependencyStorageTopology(
        builder.dependencyStorage.dependencyTopic,
        builder.dependencyStorage.dependencyTtl,
        builder.dependencyStorage.dependencyWindowSize,
        builder.dependencyStorage.enabled).get();
  }

  @Override public SpanConsumer spanConsumer() {
    checkResources();
    if (partitioningEnabled) {
      return new KafkaSpanConsumer(this);
    } else { // NoopSpanConsumer
      return spans -> Call.create(null);
    }
  }

  @Override public SpanStore spanStore() {
    checkResources();
    return new KafkaSpanStore(this);
  }

  @Override public Traces traces() {
    checkResources();
    return new KafkaSpanStore(this);
  }

  @Override public ServiceAndSpanNames serviceAndSpanNames() {
    checkResources();
    return new KafkaSpanStore(this);
  }

  @Override public AutocompleteTags autocompleteTags() {
    checkResources();
    return new KafkaAutocompleteTags(this);
  }

  void checkResources() {
    getAggregationStream();
    getTraceStorageStream();
    getDependencyStorageStream();
  }

  @Override public CheckResult check() {
    try {
      KafkaFuture<String> maybeClusterId = getAdminClient().describeCluster().clusterId();
      maybeClusterId.get(1, TimeUnit.SECONDS);
      KafkaStreams.State state = getAggregationStream().state();
      if (!state.isRunningOrRebalancing()) {
        return CheckResult.failed(
            new IllegalStateException("Aggregation stream not running. " + state));
      }
      KafkaStreams.State traceStateStore = getTraceStorageStream().state();
      if (!traceStateStore.isRunningOrRebalancing()) {
        return CheckResult.failed(
            new IllegalStateException("Store stream not running. " + traceStateStore));
      }
      KafkaStreams.State dependencyStateStore = getDependencyStorageStream().state();
      if (!dependencyStateStore.isRunningOrRebalancing()) {
        return CheckResult.failed(
            new IllegalStateException("Store stream not running. " + dependencyStateStore));
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
      if (producer != null) producer.close(Duration.ofSeconds(1));
      if (traceStoreStream != null) traceStoreStream.close(Duration.ofSeconds(1));
      if (dependencyStoreStream != null) dependencyStoreStream.close(Duration.ofSeconds(1));
      if (aggregationStream != null) aggregationStream.close(Duration.ofSeconds(1));
      if (server != null) server.close();
    } catch (Exception | Error e) {
      LOG.debug("error closing client {}", e.getMessage(), e);
    }
  }

  Producer<String, byte[]> getProducer() {
    if (producer == null) {
      synchronized (this) {
        if (producer == null) producer = new KafkaProducer<>(producerConfig);
      }
    }
    return producer;
  }

  AdminClient getAdminClient() {
    if (adminClient == null) {
      synchronized (this) {
        if (adminClient == null) adminClient = AdminClient.create(adminConfig);
      }
    }
    return adminClient;
  }

  KafkaStreams getTraceStorageStream() {
    if (traceStoreStream == null) {
      synchronized (this) {
        if (traceStoreStream == null) {
          try {
            traceStoreStream = new KafkaStreams(traceStoreTopology, traceStoreStreamConfig);
            traceStoreStream.start();
            LOG.info("Trace storage topology:\n{}", traceStoreTopology.describe());
          } catch (Exception e) {
            LOG.debug("Error starting trace storage process", e);
            traceStoreStream = null;
          }
        }
      }
    }
    return traceStoreStream;
  }

  KafkaStreams getDependencyStorageStream() {
    if (dependencyStoreStream == null) {
      synchronized (this) {
        if (dependencyStoreStream == null) {
          try {
            dependencyStoreStream =
                new KafkaStreams(dependencyStoreTopology, dependencyStoreStreamConfig);
            dependencyStoreStream.start();
            LOG.info("Dependency storage topology:\n{}", dependencyStoreTopology.describe());
          } catch (Exception e) {
            LOG.debug("Error starting dependency storage", e);
            dependencyStoreStream = null;
          }
        }
      }
    }
    return dependencyStoreStream;
  }

  KafkaStreams getAggregationStream() {
    if (aggregationStream == null) {
      synchronized (this) {
        if (aggregationStream == null) {
          try {
            aggregationStream = new KafkaStreams(aggregationTopology, aggregationStreamConfig);
            aggregationStream.start();
            LOG.info("Aggregation topology:\n{}", aggregationTopology.describe());
          } catch (Exception e) {
            LOG.debug("Error loading aggregation process", e);
            aggregationStream = null;
          }
        }
      }
    }
    return aggregationStream;
  }

  public KafkaStorageHttpService httpService() {
    return new KafkaStorageHttpService(this);
  }

  @Override public String toString() {
    return "KafkaStorage{" +
        " bootstrapServers=" + adminConfig.getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG) +
        ", spanPartitioning{ enabled=" + partitioningEnabled +
        ", spansTopic=" + partitioningSpansTopic + "}" +
        ", spanAggregation{ enabled=" + aggregationEnabled +
        ", spansTopic=" + aggregationSpansTopic +
        ", traceTopic=" + aggregationTraceTopic +
        ", dependencyTopic=" + aggregationDependencyTopic + "}" +
        ", traceStore { traceByIdQueryEnabled=" + traceByIdQueryEnabled +
        ", traceSearchEnabled=" + traceSearchEnabled +
        ", spansTopic=" + storageSpansTopic + "}" +
        ", dependencyStore { dependencyQueryEnabled=" + dependencyQueryEnabled +
        ", dependencyTopic=" + storageDependencyTopic + "}" +
        '}';
  }
}
