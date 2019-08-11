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
import org.springframework.boot.context.properties.ConfigurationProperties;
import zipkin2.storage.kafka.KafkaStorage;

@ConfigurationProperties("zipkin.storage.kafka")
public class ZipkinKafkaStorageProperties implements Serializable {
  private static final long serialVersionUID = 0L;

  private Boolean spanConsumerEnabled;
  private Boolean aggregationEnabled;
  private Boolean spanStoreEnabled;
  private Boolean ensureTopics;

  private String bootstrapServers;
  private String compressionType;

  private Long tracesRetentionScanFrequency;
  private Long tracesRetentionPeriod;
  private Long dependenciesRetentionPeriod;
  private Long dependenciesWindowSize;
  private Long traceInactivityGap;

  private String spansTopic = "zipkin-spans-v1";
  private Integer spansTopicPartitions = 1;
  private Short spansTopicReplicationFactor = 1;

  private String tracesTopic = "zipkin-traces-v1";
  private Integer tracesTopicPartitions = 1;
  private Short tracesTopicReplicationFactor = 1;

  private String dependencyLinksTopic = "zipkin-dependency-links-v1";
  private Integer dependencyLinksTopicPartitions = 1;
  private Short dependencyLinksTopicReplicationFactor = 1;

  private String storeDirectory = "/tmp/zipkin";

  KafkaStorage.Builder toBuilder() {
    KafkaStorage.Builder builder = KafkaStorage.newBuilder();
    if (spanConsumerEnabled != null) builder.spanConsumerEnabled(spanConsumerEnabled);
    if (spanStoreEnabled != null) builder.spanStoreEnabled(spanStoreEnabled);
    if (aggregationEnabled != null) builder.aggregationEnabled(aggregationEnabled);
    if (ensureTopics != null) builder.ensureTopics(ensureTopics);
    if (bootstrapServers != null) builder.bootstrapServers(bootstrapServers);
    if (compressionType != null) builder.compressionType(compressionType);
    if (traceInactivityGap != null) {
      builder.traceInactivityGap(Duration.ofMillis(traceInactivityGap));
    }
    if (tracesRetentionScanFrequency != null) {
      builder.tracesRetentionScanFrequency(Duration.ofMillis(tracesRetentionScanFrequency));
    }
    if (tracesRetentionPeriod != null) {
      builder.dependenciesRetentionPeriod(Duration.ofMillis(dependenciesRetentionPeriod));
    }
    if (dependenciesWindowSize != null) {
      builder.dependenciesWindowSize(Duration.ofMillis(dependenciesWindowSize));
    }

    return builder
        .spansTopic(KafkaStorage.Topic.builder(spansTopic)
            .partitions(spansTopicPartitions)
            .replicationFactor(spansTopicReplicationFactor)
            .build())
        .tracesTopic(KafkaStorage.Topic.builder(tracesTopic)
            .partitions(tracesTopicPartitions)
            .replicationFactor(tracesTopicReplicationFactor)
            .build())
        .dependencyLinksTopic(KafkaStorage.Topic.builder(dependencyLinksTopic)
            .partitions(dependencyLinksTopicPartitions)
            .replicationFactor(dependencyLinksTopicReplicationFactor)
            .build())
        .storeDirectory(storeDirectory);
  }

  public void setSpanConsumerEnabled(boolean spanConsumerEnabled) {
    this.spanConsumerEnabled = spanConsumerEnabled;
  }

  public void setAggregationEnabled(boolean aggregationEnabled) {
    this.aggregationEnabled = aggregationEnabled;
  }

  public boolean isEnsureTopics() {
    return ensureTopics;
  }

  public void setEnsureTopics(boolean ensureTopics) {
    this.ensureTopics = ensureTopics;
  }

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  public void setBootstrapServers(String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }

  public String getCompressionType() {
    return compressionType;
  }

  public void setCompressionType(String compressionType) {
    this.compressionType = compressionType;
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

  public Integer getSpansTopicPartitions() {
    return spansTopicPartitions;
  }

  public void setSpansTopicPartitions(Integer spansTopicPartitions) {
    this.spansTopicPartitions = spansTopicPartitions;
  }

  public Short getSpansTopicReplicationFactor() {
    return spansTopicReplicationFactor;
  }

  public void setSpansTopicReplicationFactor(Short spansTopicReplicationFactor) {
    this.spansTopicReplicationFactor = spansTopicReplicationFactor;
  }

  public Boolean getSpanConsumerEnabled() {
    return spanConsumerEnabled;
  }

  public void setSpanConsumerEnabled(Boolean spanConsumerEnabled) {
    this.spanConsumerEnabled = spanConsumerEnabled;
  }

  public Boolean getAggregationEnabled() {
    return aggregationEnabled;
  }

  public void setAggregationEnabled(Boolean aggregationEnabled) {
    this.aggregationEnabled = aggregationEnabled;
  }

  public Boolean getEnsureTopics() {
    return ensureTopics;
  }

  public void setEnsureTopics(Boolean ensureTopics) {
    this.ensureTopics = ensureTopics;
  }

  public String getTracesTopic() {
    return tracesTopic;
  }

  public void setTracesTopic(String tracesTopic) {
    this.tracesTopic = tracesTopic;
  }

  public Integer getTracesTopicPartitions() {
    return tracesTopicPartitions;
  }

  public void setTracesTopicPartitions(Integer tracesTopicPartitions) {
    this.tracesTopicPartitions = tracesTopicPartitions;
  }

  public Short getTracesTopicReplicationFactor() {
    return tracesTopicReplicationFactor;
  }

  public void setTracesTopicReplicationFactor(Short tracesTopicReplicationFactor) {
    this.tracesTopicReplicationFactor = tracesTopicReplicationFactor;
  }

  public String getDependencyLinksTopic() {
    return dependencyLinksTopic;
  }

  public void setDependencyLinksTopic(String dependencyLinksTopic) {
    this.dependencyLinksTopic = dependencyLinksTopic;
  }

  public Integer getDependencyLinksTopicPartitions() {
    return dependencyLinksTopicPartitions;
  }

  public void setDependencyLinksTopicPartitions(Integer dependencyLinksTopicPartitions) {
    this.dependencyLinksTopicPartitions = dependencyLinksTopicPartitions;
  }

  public Short getDependencyLinksTopicReplicationFactor() {
    return dependencyLinksTopicReplicationFactor;
  }

  public void setDependencyLinksTopicReplicationFactor(Short dependencyLinksTopicReplicationFactor) {
    this.dependencyLinksTopicReplicationFactor = dependencyLinksTopicReplicationFactor;
  }

  public String getStoreDirectory() {
    return storeDirectory;
  }

  public void setStoreDirectory(String storeDirectory) {
    this.storeDirectory = storeDirectory;
  }

  public Long getDependenciesRetentionPeriod() {
    return dependenciesRetentionPeriod;
  }

  public void setDependenciesRetentionPeriod(Long dependenciesRetentionPeriod) {
    this.dependenciesRetentionPeriod = dependenciesRetentionPeriod;
  }

  public Long getDependenciesWindowSize() {
    return dependenciesWindowSize;
  }

  public void setDependenciesWindowSize(Long dependenciesWindowSize) {
    this.dependenciesWindowSize = dependenciesWindowSize;
  }

  public Boolean getSpanStoreEnabled() {
    return spanStoreEnabled;
  }

  public void setSpanStoreEnabled(Boolean spanStoreEnabled) {
    this.spanStoreEnabled = spanStoreEnabled;
  }
}
