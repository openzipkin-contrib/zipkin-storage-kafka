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
import org.apache.kafka.common.record.CompressionType;
import org.springframework.boot.context.properties.ConfigurationProperties;
import zipkin2.storage.kafka.KafkaStorage;

@ConfigurationProperties("zipkin.storage.kafka")
public class ZipkinKafkaStorageProperties implements Serializable {
  private static final long serialVersionUID = 0L;

  private boolean spanConsumerEnabled = true;
  private boolean aggregationEnabled = true;
  private boolean spanStoreEnabled = true;

  private boolean ensureTopics = true;
  private String bootstrapServers = "localhost:9092";
  private String compressionType = CompressionType.NONE.name();

  private Long retentionScanFrequency = Duration.ofDays(1).toMillis();
  private Long retentionMaxAge = Duration.ofDays(7).toMillis();
  private Long traceInactivityGap = Duration.ofMinutes(1).toMillis();

  private String spansTopic = "zipkin-spans-v1";
  private Integer spansTopicPartitions = 1;
  private Short spansTopicReplicationFactor = 1;
  private String dependencyLinksTopic = "zipkin-dependency-links-v1";
  private Integer dependencyLinksTopicPartitions = 1;
  private Short dependencyLinksReplicationFactor = 1;

  private String storeDirectory = "/tmp/zipkin";

  KafkaStorage.Builder toBuilder() {
    return KafkaStorage.newBuilder()
        .spanConsumerEnabled(spanConsumerEnabled)
        .aggregationEnabled(aggregationEnabled)
        .spanStoreEnabled(spanStoreEnabled)
        .ensureTopics(ensureTopics)
        .bootstrapServers(bootstrapServers)
        .compressionType(compressionType)
        .retentionMaxAge(Duration.ofMillis(retentionMaxAge))
        .retentionScanFrequency(Duration.ofMillis(retentionScanFrequency))
        .traceInactivityGap(Duration.ofMillis(traceInactivityGap))
        .spansTopic(KafkaStorage.Topic.builder(spansTopic)
            .partitions(spansTopicPartitions)
            .replicationFactor(spansTopicReplicationFactor)
            .build())
        .dependenciesTopic(KafkaStorage.Topic.builder(dependencyLinksTopic)
            .partitions(dependencyLinksTopicPartitions)
            .replicationFactor(dependencyLinksReplicationFactor)
            .build())
        .storeDirectory(storeDirectory);
  }

  public boolean isSpanConsumerEnabled() {
    return spanConsumerEnabled;
  }

  public void setSpanConsumerEnabled(boolean spanConsumerEnabled) {
    this.spanConsumerEnabled = spanConsumerEnabled;
  }

  public boolean isAggregationEnabled() {
    return aggregationEnabled;
  }

  public void setAggregationEnabled(boolean aggregationEnabled) {
    this.aggregationEnabled = aggregationEnabled;
  }

  public boolean isSpanStoreEnabled() {
    return spanStoreEnabled;
  }

  public void setSpanStoreEnabled(boolean spanStoreEnabled) {
    this.spanStoreEnabled = spanStoreEnabled;
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

  public Long getRetentionScanFrequency() {
    return retentionScanFrequency;
  }

  public void setRetentionScanFrequency(Long retentionScanFrequency) {
    this.retentionScanFrequency = retentionScanFrequency;
  }

  public Long getRetentionMaxAge() {
    return retentionMaxAge;
  }

  public void setRetentionMaxAge(Long retentionMaxAge) {
    this.retentionMaxAge = retentionMaxAge;
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

  public Short getDependencyLinksReplicationFactor() {
    return dependencyLinksReplicationFactor;
  }

  public void setDependencyLinksReplicationFactor(Short dependencyLinksReplicationFactor) {
    this.dependencyLinksReplicationFactor = dependencyLinksReplicationFactor;
  }

  public String getStoreDirectory() {
    return storeDirectory;
  }

  public void setStoreDirectory(String storeDirectory) {
    this.storeDirectory = storeDirectory;
  }
}
