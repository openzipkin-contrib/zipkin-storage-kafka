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

  private String bootstrapServers;

  private Long tracesRetentionScanFrequency;
  private Long tracesRetentionPeriod;
  private Long dependenciesRetentionPeriod;
  private Long tracesInactivityGap;

  private String spansTopic = "zipkin-spans";
  private String tracesTopic = "zipkin-traces";
  private String dependenciesTopic = "zipkin-dependencies";

  private String storeDirectory = "/tmp/zipkin";

  KafkaStorage.Builder toBuilder() {
    KafkaStorage.Builder builder = KafkaStorage.newBuilder();
    if (spanConsumerEnabled != null) builder.spanConsumerEnabled(spanConsumerEnabled);
    if (spanStoreEnabled != null) builder.spanStoreEnabled(spanStoreEnabled);
    if (aggregationEnabled != null) builder.aggregationEnabled(aggregationEnabled);
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

    return builder
        .spansTopicName(spansTopic)
        .tracesTopicName(tracesTopic)
        .dependenciesTopicName(dependenciesTopic)
        .storeDirectory(storeDirectory);
  }

  public void setSpanConsumerEnabled(boolean spanConsumerEnabled) {
    this.spanConsumerEnabled = spanConsumerEnabled;
  }

  public void setAggregationEnabled(boolean aggregationEnabled) {
    this.aggregationEnabled = aggregationEnabled;
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

  public Boolean getAggregationEnabled() {
    return aggregationEnabled;
  }

  public void setAggregationEnabled(Boolean aggregationEnabled) {
    this.aggregationEnabled = aggregationEnabled;
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

  public Boolean getSpanStoreEnabled() {
    return spanStoreEnabled;
  }

  public void setSpanStoreEnabled(Boolean spanStoreEnabled) {
    this.spanStoreEnabled = spanStoreEnabled;
  }
}
