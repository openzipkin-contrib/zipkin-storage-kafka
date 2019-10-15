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

import com.linecorp.armeria.server.Server;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zipkin2.Call;
import zipkin2.CheckResult;
import zipkin2.storage.AutocompleteTags;
import zipkin2.storage.ServiceAndSpanNames;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StorageComponent;
import zipkin2.storage.Traces;
import zipkin2.storage.kafka.internal.NoopServiceAndSpanNames;
import zipkin2.storage.kafka.internal.NoopSpanStore;
import zipkin2.storage.kafka.streams.AggregationTopologySupplier;
import zipkin2.storage.kafka.streams.DependencyStoreTopologySupplier;
import zipkin2.storage.kafka.streams.TraceStoreTopologySupplier;

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
  static final Logger LOG = LogManager.getLogger();

  public static KafkaStorageBuilder newBuilder() {
    return new KafkaStorageBuilder();
  }

  // Kafka Storage modes
  final boolean spanConsumerEnabled, searchEnabled;
  // Autocomplete Tags
  final List<String> autocompleteKeys;
  // Kafka Storage configs
  final String storageDir;
  final long minTracesStored;
  final String hostname;
  final int httpPort;
  // Kafka Topics
  final String partitionedSpansTopic;
  final String aggregationSpansTopic, aggregationTraceTopic, aggregationDependencyTopic;
  final String storeSpansTopic, storeDependencyTopic;
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
  volatile KafkaStreams traceAggregationStream, traceStoreStream, dependencyStoreStream;
  volatile Server server;
  volatile boolean closeCalled;

  KafkaStorage(KafkaStorageBuilder builder) {
    // Kafka Storage modes
    this.spanConsumerEnabled = builder.spanConsumerEnabled;
    this.searchEnabled = builder.searchEnabled;
    // Autocomplete tags
    this.autocompleteKeys = builder.autocompleteKeys;
    // Kafka Topics config
    this.partitionedSpansTopic = builder.partitionedSpansTopic;
    this.aggregationSpansTopic = builder.aggregationSpansTopic;
    this.aggregationTraceTopic = builder.aggregationTraceTopic;
    this.aggregationDependencyTopic = builder.aggregationDependencyTopic;
    this.storeSpansTopic = builder.storeSpansTopic;
    this.storeDependencyTopic = builder.storeDependencyTopic;
    // Storage directories
    this.storageDir = builder.storageDir;
    this.minTracesStored = builder.minTracesStored;
    this.hostname = builder.hostname;
    this.httpPort = builder.httpPort;
    this.httpBaseUrl = builder.httpBaseUrl;
    // Kafka Configs
    this.adminConfig = builder.adminConfig;
    this.producerConfig = builder.producerConfig;
    this.aggregationStreamConfig = builder.aggregationStreamConfig;
    this.traceStoreStreamConfig = builder.traceStoreStreamConfig;
    this.dependencyStoreStreamConfig = builder.dependencyStoreStreamConfig;

    aggregationTopology = new AggregationTopologySupplier(
        aggregationSpansTopic,
        aggregationTraceTopic,
        aggregationDependencyTopic,
        builder.traceTimeout).get();
    traceStoreTopology = new TraceStoreTopologySupplier(
        storeSpansTopic,
        autocompleteKeys,
        builder.traceTtl,
        builder.traceTtlCheckInterval,
        builder.minTracesStored).get();
    dependencyStoreTopology = new DependencyStoreTopologySupplier(
        storeDependencyTopic,
        builder.dependencyTtl,
        builder.dependencyWindowSize).get();
  }

  @Override public SpanConsumer spanConsumer() {
    checkResources();
    if (spanConsumerEnabled) {
      return new KafkaSpanConsumer(this);
    } else { // NoopSpanConsumer
      return list -> Call.create(null);
    }
  }

  @Override public SpanStore spanStore() {
    checkResources();
    if (searchEnabled) { // not exactly correct. See https://github.com/openzipkin/zipkin/pull/2803
      return new KafkaSpanStore(this);
    } else {
      return new NoopSpanStore();
    }
  }

  @Override public Traces traces() {
    checkResources();
    return new KafkaSpanStore(this);
  }

  @Override public ServiceAndSpanNames serviceAndSpanNames() {
    checkResources();
    if (searchEnabled) {
      return new KafkaSpanStore(this);
    } else {
      return new NoopServiceAndSpanNames();
    }
  }


  @Override public AutocompleteTags autocompleteTags() {
    checkResources();
    if (searchEnabled) {
      return new KafkaAutocompleteTags(this);
    } else {
      return super.autocompleteTags();
    }
  }

  void checkResources() {
    if (spanConsumerEnabled) {
      getAggregationStream();
    }
    if (searchEnabled) {
      getTraceStoreStream();
      getDependencyStoreStream();
      getServer();
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
        KafkaStreams.State stateTraceStore = getTraceStoreStream().state();
        if (!stateTraceStore.isRunning()) {
          return CheckResult.failed(
              new IllegalStateException("Store stream not running. " + stateTraceStore));
        }
        KafkaStreams.State stateDependencyStore = getDependencyStoreStream().state();
        if (!stateDependencyStore.isRunning()) {
          return CheckResult.failed(
              new IllegalStateException("Store stream not running. " + stateDependencyStore));
        }
        if (!getServer().activePort().isPresent()) {
          return CheckResult.failed(
              new IllegalStateException(
                  "Storage HTTP server not running. " + stateDependencyStore));
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
        producer.close(Duration.ofSeconds(1));
      }
      if (traceStoreStream != null) {
        traceStoreStream.close(Duration.ofSeconds(1));
      }
      if (dependencyStoreStream != null) {
        dependencyStoreStream.close(Duration.ofSeconds(1));
      }
      if (traceAggregationStream != null) {
        traceAggregationStream.close(Duration.ofSeconds(1));
      }
      if (server != null) server.close();
    } catch (Exception | Error e) {
      LOG.debug("error closing client {}", e.getMessage(), e);
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
          try {
            traceStoreStream = new KafkaStreams(traceStoreTopology, traceStoreStreamConfig);
            traceStoreStream.start();
          } catch (Exception e) {
            LOG.debug("Error starting trace store process", e);
            traceStoreStream = null;
          }
        }
      }
    }
    return traceStoreStream;
  }

  KafkaStreams getDependencyStoreStream() {
    if (dependencyStoreStream == null) {
      synchronized (this) {
        if (dependencyStoreStream == null) {
          try {
            dependencyStoreStream =
                new KafkaStreams(dependencyStoreTopology, dependencyStoreStreamConfig);
            dependencyStoreStream.start();
          } catch (Exception e) {
            LOG.debug("Error starting dependency store", e);
            dependencyStoreStream = null;
          }
        }
      }
    }
    return dependencyStoreStream;
  }

  KafkaStreams getAggregationStream() {
    if (traceAggregationStream == null) {
      synchronized (this) {
        if (traceAggregationStream == null) {
          try {
            traceAggregationStream =
                new KafkaStreams(aggregationTopology, aggregationStreamConfig);
            traceAggregationStream.start();
          } catch (Exception e) {
            LOG.debug("Error loading aggregation process", e);
            traceAggregationStream = null;
          }
        }
      }
    }
    return traceAggregationStream;
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  Server getServer() {
    if (server == null) {
      synchronized (this) {
        if (server == null) {
          try {
            server = Server.builder()
              .http(httpPort)
              .annotatedService(new KafkaStoreHttpService(this))
              .build();
            server.start();
          } catch (Exception e) {
            LOG.error("Error starting http server", e);
            server = null;
          }
        }
      }
    }
    return server;
  }

  @Override public String toString() {
    return "KafkaStorage{" +
        "httpPort=" + httpPort +
        ", spanConsumerEnabled=" + spanConsumerEnabled +
        ", searchEnabled=" + searchEnabled +
        ", storageDir='" + storageDir + '\'' +
        '}';
  }
}
