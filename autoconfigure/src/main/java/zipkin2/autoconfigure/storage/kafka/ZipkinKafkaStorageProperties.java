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
  private boolean spanStoreEnabled = true;

  private boolean ensureTopics = true;
  private String bootstrapServers = "localhost:9092";
  private String compressionType = CompressionType.NONE.name();

  private Long retentionScanFrequency = Duration.ofDays(1).toMillis();
  private Long retentionMaxAge = Duration.ofDays(7).toMillis();
  private Long traceInactivityGap = Duration.ofMinutes(5).toMillis();

  private String spansTopic = "zipkin-spans_v1";
  private Integer spansTopicPartitions = 1;
  private Short spansTopicReplicationFactor = 1;
  private String tracesTopic = "zipkin-traces_v1";
  private Integer tracesTopicPartitions = 1;
  private Short tracesTopicReplicationFactor = 1;
  private String dependenciesTopic = "zipkin-dependencies_v1";
  private Integer dependenciesTopicPartitions = 1;
  private Short dependenciesTopicReplicationFactor = 1;
  private String traceSpansTopic = "zipkin-trace-spans_v1";
  private Integer traceSpansTopicPartitions = 1;
  private Short traceSpansTopicReplicationFactor = 1;

  private String storeDirectory = "/tmp/zipkin";

  KafkaStorage.Builder toBuilder() {
    return KafkaStorage.newBuilder()
        .spanConsumerEnabled(spanConsumerEnabled)
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
        .tracesTopic(KafkaStorage.Topic.builder(tracesTopic)
            .partitions(tracesTopicPartitions)
            .replicationFactor(tracesTopicReplicationFactor)
            .build())
        .dependenciesTopic(KafkaStorage.Topic.builder(dependenciesTopic)
            .partitions(dependenciesTopicPartitions)
            .replicationFactor(dependenciesTopicReplicationFactor)
            .build())
        .traceSpansTopic(KafkaStorage.Topic.builder(traceSpansTopic)
            .partitions(traceSpansTopicPartitions)
            .replicationFactor(traceSpansTopicReplicationFactor)
            .build())
        .storeDirectory(storeDirectory);
  }

  public boolean isSpanConsumerEnabled() {
    return spanConsumerEnabled;
  }

  public void setSpanConsumerEnabled(boolean spanConsumerEnabled) {
    this.spanConsumerEnabled = spanConsumerEnabled;
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

  public String getSpansTopic() {
    return spansTopic;
  }

  public void setSpansTopic(String spansTopic) {
    this.spansTopic = spansTopic;
  }

  public String getTracesTopic() {
    return tracesTopic;
  }

  public void setTracesTopic(String tracesTopic) {
    this.tracesTopic = tracesTopic;
  }

  public String getTraceSpansTopic() {
    return traceSpansTopic;
  }

  public void setTraceSpansTopic(String traceSpansTopic) {
    this.traceSpansTopic = traceSpansTopic;
  }

  public String getStoreDirectory() {
    return storeDirectory;
  }

  public void setStoreDirectory(String storeDirectory) {
    this.storeDirectory = storeDirectory;
  }

  public String getCompressionType() {
    return compressionType;
  }

  public void setCompressionType(String compressionType) {
    this.compressionType = compressionType;
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

  public Integer getTraceSpansTopicPartitions() {
    return traceSpansTopicPartitions;
  }

  public void setTraceSpansTopicPartitions(Integer traceSpansTopicPartitions) {
    this.traceSpansTopicPartitions = traceSpansTopicPartitions;
  }

  public Short getTraceSpansTopicReplicationFactor() {
    return traceSpansTopicReplicationFactor;
  }

  public void setTraceSpansTopicReplicationFactor(Short traceSpansTopicReplicationFactor) {
    this.traceSpansTopicReplicationFactor = traceSpansTopicReplicationFactor;
  }

  public String getDependenciesTopic() {
    return dependenciesTopic;
  }

  public void setDependenciesTopic(String dependenciesTopic) {
    this.dependenciesTopic = dependenciesTopic;
  }

  public Integer getDependenciesTopicPartitions() {
    return dependenciesTopicPartitions;
  }

  public void setDependenciesTopicPartitions(Integer dependenciesTopicPartitions) {
    this.dependenciesTopicPartitions = dependenciesTopicPartitions;
  }

  public Short getDependenciesTopicReplicationFactor() {
    return dependenciesTopicReplicationFactor;
  }

  public void setDependenciesTopicReplicationFactor(Short dependenciesTopicReplicationFactor) {
    this.dependenciesTopicReplicationFactor = dependenciesTopicReplicationFactor;
  }
}
