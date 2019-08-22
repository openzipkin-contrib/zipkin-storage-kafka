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
package zipkin2.autoconfigure.storage.kafka;

import java.io.Serializable;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import zipkin2.storage.kafka.KafkaStorage;

@ConfigurationProperties("zipkin.storage.kafka")
public class ZipkinKafkaStorageProperties implements Serializable {
  private static final long serialVersionUID = 0L;

  private Boolean spanConsumerEnabled;

  private String bootstrapServers;

  private Long traceRetentionScanFrequency;
  private Long traceRetentionPeriod;
  private Long traceInactivityGap;

  private Long dependencyRetentionPeriod;

  private String spansTopic;
  private String traceTopic;
  private String dependencyTopic;

  private String storeDir;

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

  KafkaStorage.Builder toBuilder() {
    KafkaStorage.Builder builder = KafkaStorage.newBuilder();
    if (spanConsumerEnabled != null) builder.spanConsumerEnabled(spanConsumerEnabled);
    if (bootstrapServers != null) builder.bootstrapServers(bootstrapServers);
    if (traceInactivityGap != null) {
      builder.traceInactivityGap(Duration.ofMillis(traceInactivityGap));
    }
    if (traceRetentionScanFrequency != null) {
      builder.traceGcInterval(Duration.ofMillis(traceRetentionScanFrequency));
    }
    if (traceRetentionPeriod != null) {
      builder.traceTtl(Duration.ofMillis(traceRetentionPeriod));
    }
    if (dependencyRetentionPeriod != null) {
      builder.dependencyTtl(Duration.ofMillis(dependencyRetentionPeriod));
    }
    if (aggregationStreamAppId != null) builder.aggregationStreamAppId(aggregationStreamAppId);
    if (traceStoreStreamAppId != null) builder.aggregationStreamAppId(traceStoreStreamAppId);
    if (dependencyStoreStreamAppId != null) {
      builder.aggregationStreamAppId(dependencyStoreStreamAppId);
    }
    if (storeDir != null) builder.storeDirectory(storeDir);
    if (spansTopic != null) builder.spansTopicName(spansTopic);
    if (traceTopic != null) builder.tracesTopicName(traceTopic);
    if (dependencyTopic != null) builder.dependenciesTopicName(dependencyTopic);
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

  public void setSpanConsumerEnabled(boolean spanConsumerEnabled) {
    this.spanConsumerEnabled = spanConsumerEnabled;
  }

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  public void setBootstrapServers(String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }

  public Long getTraceRetentionScanFrequency() {
    return traceRetentionScanFrequency;
  }

  public void setTraceRetentionScanFrequency(Long traceRetentionScanFrequency) {
    this.traceRetentionScanFrequency = traceRetentionScanFrequency;
  }

  public Long getTraceRetentionPeriod() {
    return traceRetentionPeriod;
  }

  public void setTraceRetentionPeriod(Long traceRetentionPeriod) {
    this.traceRetentionPeriod = traceRetentionPeriod;
  }

  public Long getTraceInactivityGap() {
    return traceInactivityGap;
  }

  public void setTraceInactivityGap(Long traceInactivityGap) {
    this.traceInactivityGap = traceInactivityGap;
  }

  public String getSpansTopic() {
    return spansTopic;
  }

  public void setSpansTopic(String spansTopic) {
    this.spansTopic = spansTopic;
  }

  public Boolean getSpanConsumerEnabled() {
    return spanConsumerEnabled;
  }

  public void setSpanConsumerEnabled(Boolean spanConsumerEnabled) {
    this.spanConsumerEnabled = spanConsumerEnabled;
  }

  public String getTraceTopic() {
    return traceTopic;
  }

  public void setTraceTopic(String traceTopic) {
    this.traceTopic = traceTopic;
  }

  public String getDependencyTopic() {
    return dependencyTopic;
  }

  public void setDependencyTopic(String dependencyTopic) {
    this.dependencyTopic = dependencyTopic;
  }

  public String getStoreDir() {
    return storeDir;
  }

  public void setStoreDir(String storeDir) {
    this.storeDir = storeDir;
  }

  public Long getDependencyRetentionPeriod() {
    return dependencyRetentionPeriod;
  }

  public void setDependencyRetentionPeriod(Long dependencyRetentionPeriod) {
    this.dependencyRetentionPeriod = dependencyRetentionPeriod;
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
}
