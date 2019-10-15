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
package zipkin2.autoconfigure.storage.kafka;

import java.io.Serializable;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import zipkin2.storage.kafka.KafkaStorage;
import zipkin2.storage.kafka.KafkaStorageBuilder;

@ConfigurationProperties("zipkin.storage.kafka")
public class ZipkinKafkaStorageProperties implements Serializable {
  private static final long serialVersionUID = 0L;

  private Boolean spanConsumerEnabled;

  private String hostname;

  private String bootstrapServers;

  private Long traceTtlCheckInterval;
  private Long traceTtl;
  private Long traceTimeout;

  private Long dependencyTtl;

  private String partitionedSpansTopic;
  private String aggregationSpansTopic;
  private String aggregationTraceTopic;
  private String aggregationDependencyTopic;
  private String storeSpansTopic;
  private String storeDependencyTopic;

  private String storageDir;

  private String aggregationStreamAppId;
  private String traceStoreStreamAppId;
  private String dependencyStoreStreamAppId;

  /**
   * Additional Kafka configuration.
   */
  private Map<String, String> adminOverrides = new LinkedHashMap<>();
  private Map<String, String> producerOverrides = new LinkedHashMap<>();
  private Map<String, String> aggregationStreamOverrides = new LinkedHashMap<>();
  private Map<String, String> traceStoreStreamOverrides = new LinkedHashMap<>();
  private Map<String, String> dependencyStoreStreamOverrides = new LinkedHashMap<>();

  KafkaStorageBuilder toBuilder() {
    KafkaStorageBuilder builder = KafkaStorage.newBuilder();
    if (spanConsumerEnabled != null) builder.spanConsumerEnabled(spanConsumerEnabled);
    if (hostname != null) builder.hostname(hostname);
    if (bootstrapServers != null) builder.bootstrapServers(bootstrapServers);
    if (traceTimeout != null) {
      builder.traceTimeout(Duration.ofMillis(traceTimeout));
    }
    if (traceTtlCheckInterval != null) {
      builder.traceTtlCheckInterval(Duration.ofMillis(traceTtlCheckInterval));
    }
    if (traceTtl != null) {
      builder.traceTtl(Duration.ofMillis(traceTtl));
    }
    if (dependencyTtl != null) {
      builder.dependencyTtl(Duration.ofMillis(dependencyTtl));
    }
    if (aggregationStreamAppId != null) builder.aggregationStreamAppId(aggregationStreamAppId);
    if (traceStoreStreamAppId != null) builder.aggregationStreamAppId(traceStoreStreamAppId);
    if (dependencyStoreStreamAppId != null) {
      builder.aggregationStreamAppId(dependencyStoreStreamAppId);
    }
    if (storageDir != null) builder.storageDir(storageDir);
    if (partitionedSpansTopic != null) builder.partitionedSpansTopic(partitionedSpansTopic);
    if (aggregationSpansTopic != null) builder.aggregationSpansTopic(aggregationSpansTopic);
    if (aggregationTraceTopic != null) builder.aggregationTraceTopic(aggregationTraceTopic);
    if (aggregationDependencyTopic != null) {
      builder.aggregationDependencyTopic(aggregationDependencyTopic);
    }
    if (storeSpansTopic != null) builder.storeSpansTopic(storeSpansTopic);
    if (storeDependencyTopic != null) builder.storeDependencyTopic(storeDependencyTopic);
    if (adminOverrides != null) builder.adminOverrides(adminOverrides);
    if (producerOverrides != null) builder.producerOverrides(producerOverrides);
    if (aggregationStreamOverrides != null) {
      builder.aggregationStreamOverrides(aggregationStreamOverrides);
    }
    if (traceStoreStreamOverrides != null) {
      builder.traceStoreStreamOverrides(traceStoreStreamOverrides);
    }
    if (dependencyStoreStreamOverrides != null) {
      builder.dependencyStoreStreamOverrides(dependencyStoreStreamOverrides);
    }
    if (aggregationStreamAppId != null) builder.aggregationStreamAppId(aggregationStreamAppId);
    if (traceStoreStreamAppId != null) builder.traceStoreStreamAppId(traceStoreStreamAppId);
    if (dependencyStoreStreamAppId != null) {
      builder.dependencyStoreStreamAppId(dependencyStoreStreamAppId);
    }

    return builder;
  }

  public Boolean getSpanConsumerEnabled() {
    return spanConsumerEnabled;
  }

  public void setSpanConsumerEnabled(Boolean spanConsumerEnabled) {
    this.spanConsumerEnabled = spanConsumerEnabled;
  }

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  public void setBootstrapServers(String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }

  public String getHostname() {
    return hostname;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  public Long getTraceTtlCheckInterval() {
    return traceTtlCheckInterval;
  }

  public void setTraceTtlCheckInterval(Long traceTtlCheckInterval) {
    this.traceTtlCheckInterval = traceTtlCheckInterval;
  }

  public Long getTraceTtl() {
    return traceTtl;
  }

  public void setTraceTtl(Long traceTtl) {
    this.traceTtl = traceTtl;
  }

  public Long getTraceTimeout() {
    return traceTimeout;
  }

  public void setTraceTimeout(Long traceTimeout) {
    this.traceTimeout = traceTimeout;
  }

  public String getAggregationSpansTopic() {
    return aggregationSpansTopic;
  }

  public void setAggregationSpansTopic(String aggregationSpansTopic) {
    this.aggregationSpansTopic = aggregationSpansTopic;
  }

  public String getAggregationTraceTopic() {
    return aggregationTraceTopic;
  }

  public void setAggregationTraceTopic(String aggregationTraceTopic) {
    this.aggregationTraceTopic = aggregationTraceTopic;
  }

  public String getAggregationDependencyTopic() {
    return aggregationDependencyTopic;
  }

  public void setAggregationDependencyTopic(String aggregationDependencyTopic) {
    this.aggregationDependencyTopic = aggregationDependencyTopic;
  }

  public String getStorageDir() {
    return storageDir;
  }

  public void setStorageDir(String storageDir) {
    this.storageDir = storageDir;
  }

  public Long getDependencyTtl() {
    return dependencyTtl;
  }

  public void setDependencyTtl(Long dependencyTtl) {
    this.dependencyTtl = dependencyTtl;
  }

  public Map<String, String> getAdminOverrides() {
    return adminOverrides;
  }

  public void setAdminOverrides(Map<String, String> adminOverrides) {
    this.adminOverrides = adminOverrides;
  }

  public Map<String, String> getProducerOverrides() {
    return producerOverrides;
  }

  public void setProducerOverrides(Map<String, String> producerOverrides) {
    this.producerOverrides = producerOverrides;
  }

  public Map<String, String> getAggregationStreamOverrides() {
    return aggregationStreamOverrides;
  }

  public void setAggregationStreamOverrides(
    Map<String, String> aggregationStreamOverrides) {
    this.aggregationStreamOverrides = aggregationStreamOverrides;
  }

  public Map<String, String> getTraceStoreStreamOverrides() {
    return traceStoreStreamOverrides;
  }

  public void setTraceStoreStreamOverrides(
    Map<String, String> traceStoreStreamOverrides) {
    this.traceStoreStreamOverrides = traceStoreStreamOverrides;
  }

  public Map<String, String> getDependencyStoreStreamOverrides() {
    return dependencyStoreStreamOverrides;
  }

  public void setDependencyStoreStreamOverrides(
    Map<String, String> dependencyStoreStreamOverrides) {
    this.dependencyStoreStreamOverrides = dependencyStoreStreamOverrides;
  }

  public String getAggregationStreamAppId() {
    return aggregationStreamAppId;
  }

  public void setAggregationStreamAppId(String aggregationStreamAppId) {
    this.aggregationStreamAppId = aggregationStreamAppId;
  }

  public String getTraceStoreStreamAppId() {
    return traceStoreStreamAppId;
  }

  public void setTraceStoreStreamAppId(String traceStoreStreamAppId) {
    this.traceStoreStreamAppId = traceStoreStreamAppId;
  }

  public String getDependencyStoreStreamAppId() {
    return dependencyStoreStreamAppId;
  }

  public void setDependencyStoreStreamAppId(String dependencyStoreStreamAppId) {
    this.dependencyStoreStreamAppId = dependencyStoreStreamAppId;
  }

  public String getPartitionedSpansTopic() {
    return partitionedSpansTopic;
  }

  public void setPartitionedSpansTopic(String partitionedSpansTopic) {
    this.partitionedSpansTopic = partitionedSpansTopic;
  }

  public String getStoreSpansTopic() {
    return storeSpansTopic;
  }

  public void setStoreSpansTopic(String storeSpansTopic) {
    this.storeSpansTopic = storeSpansTopic;
  }

  public String getStoreDependencyTopic() {
    return storeDependencyTopic;
  }

  public void setStoreDependencyTopic(String storeDependencyTopic) {
    this.storeDependencyTopic = storeDependencyTopic;
  }
}
