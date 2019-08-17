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

  private Long tracesRetentionScanFrequency;
  private Long tracesRetentionPeriod;
  private Long dependenciesRetentionPeriod;
  private Long tracesInactivityGap;

  private String spansTopic;
  private String tracesTopic;
  private String dependenciesTopic;

  private String storeDir;

  /**
   * Additional Kafka configuration.
   */
  private Map<String, String> adminOverrides = new LinkedHashMap<>();
  private Map<String, String> producerOverrides = new LinkedHashMap<>();
  private Map<String, String> aggregationStreamOverrides = new LinkedHashMap<>();
  private Map<String, String> storeStreamOverrides = new LinkedHashMap<>();

  KafkaStorage.Builder toBuilder() {
    KafkaStorage.Builder builder = KafkaStorage.newBuilder();
    if (spanConsumerEnabled != null) builder.spanConsumerEnabled(spanConsumerEnabled);
    if (bootstrapServers != null) builder.bootstrapServers(bootstrapServers);
    if (tracesInactivityGap != null) {
      builder.tracesInactivityGap(Duration.ofMillis(tracesInactivityGap));
    }
    if (tracesRetentionScanFrequency != null) {
      builder.tracesRetentionScanFrequency(Duration.ofMillis(tracesRetentionScanFrequency));
    }
    if (tracesRetentionPeriod != null) {
      builder.tracesRetentionRetention(Duration.ofMillis(tracesRetentionPeriod));
    }
    if (dependenciesRetentionPeriod != null) {
      builder.dependenciesRetentionPeriod(Duration.ofMillis(dependenciesRetentionPeriod));
    }
    if (storeDir != null) builder.storeDirectory(storeDir);
    if (spansTopic != null) builder.spansTopicName(spansTopic);
    if (tracesTopic != null) builder.tracesTopicName(tracesTopic);
    if (dependenciesTopic != null) builder.dependenciesTopicName(dependenciesTopic);
    if (adminOverrides != null) builder.adminOverrides(adminOverrides);
    if (producerOverrides != null) builder.producerOverrides(producerOverrides);
    if (aggregationStreamOverrides != null) {
      builder.aggregationStreamOverrides(aggregationStreamOverrides);
    }
    if (storeStreamOverrides != null) builder.storeStreamOverrides(storeStreamOverrides);

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

  public Long getTracesRetentionScanFrequency() {
    return tracesRetentionScanFrequency;
  }

  public void setTracesRetentionScanFrequency(Long tracesRetentionScanFrequency) {
    this.tracesRetentionScanFrequency = tracesRetentionScanFrequency;
  }

  public Long getTracesRetentionPeriod() {
    return tracesRetentionPeriod;
  }

  public void setTracesRetentionPeriod(Long tracesRetentionPeriod) {
    this.tracesRetentionPeriod = tracesRetentionPeriod;
  }

  public Long getTracesInactivityGap() {
    return tracesInactivityGap;
  }

  public void setTracesInactivityGap(Long tracesInactivityGap) {
    this.tracesInactivityGap = tracesInactivityGap;
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

  public String getTracesTopic() {
    return tracesTopic;
  }

  public void setTracesTopic(String tracesTopic) {
    this.tracesTopic = tracesTopic;
  }

  public String getDependenciesTopic() {
    return dependenciesTopic;
  }

  public void setDependenciesTopic(String dependenciesTopic) {
    this.dependenciesTopic = dependenciesTopic;
  }

  public String getStoreDir() {
    return storeDir;
  }

  public void setStoreDir(String storeDir) {
    this.storeDir = storeDir;
  }

  public Long getDependenciesRetentionPeriod() {
    return dependenciesRetentionPeriod;
  }

  public void setDependenciesRetentionPeriod(Long dependenciesRetentionPeriod) {
    this.dependenciesRetentionPeriod = dependenciesRetentionPeriod;
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

  public Map<String, String> getStoreStreamOverrides() {
    return storeStreamOverrides;
  }

  public void setStoreStreamOverrides(
      Map<String, String> storeStreamOverrides) {
    this.storeStreamOverrides = storeStreamOverrides;
  }
}
