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
package zipkin2.module.storage.kafka;

import java.io.Serializable;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import zipkin2.storage.kafka.KafkaStorage;
import zipkin2.storage.kafka.KafkaStorageBuilder;
import zipkin2.storage.kafka.KafkaStorageBuilder.DependencyStorageBuilder;
import zipkin2.storage.kafka.KafkaStorageBuilder.SpanAggregationBuilder;
import zipkin2.storage.kafka.KafkaStorageBuilder.SpanPartitioningBuilder;
import zipkin2.storage.kafka.KafkaStorageBuilder.TraceStorageBuilder;

@ConfigurationProperties("zipkin.storage.kafka")
public class ZipkinKafkaStorageProperties implements Serializable {
  private static final long serialVersionUID = 0L;
  private String hostname;
  private String storageDir;
  // Kafka properties
  private String bootstrapServers;
  private Map<String, String> overrides = new LinkedHashMap<>();
  // Component-specific properties
  private SpanPartitioning spanPartitioning = new SpanPartitioning();
  private SpanAggregation spanAggregation = new SpanAggregation();
  private TraceStorage traceStorage = new TraceStorage();
  private DependencyStorage dependencyStorage = new DependencyStorage();

  KafkaStorageBuilder toBuilder() {
    KafkaStorageBuilder builder = KafkaStorage.newBuilder();
    builder.spanPartitioningBuilder(spanPartitioning.toBuilder());
    builder.spanAggregationBuilder(spanAggregation.toBuilder());
    builder.traceStorageBuilder(traceStorage.toBuilder());
    builder.dependencyStorageBuilder(dependencyStorage.toBuilder());
    if (hostname != null) builder.storageHostInfo(hostname, 9412); //TODO to be obtained from zipkin server port
    if (storageDir != null) builder.storageStateDir(storageDir);
    if (bootstrapServers != null) builder.bootstrapServers(bootstrapServers);
    if (overrides != null) builder.overrides(overrides);
    return builder;
  }

  public String getHostname() {
    return hostname;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  public void setBootstrapServers(String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }

  public String getStorageDir() {
    return storageDir;
  }

  public void setStorageDir(String storageDir) {
    this.storageDir = storageDir;
  }

  public Map<String, String> getOverrides() {
    return overrides;
  }

  public void setOverrides(Map<String, String> overrides) {
    this.overrides = overrides;
  }

  public SpanPartitioning getSpanPartitioning() {
    return spanPartitioning;
  }

  public void setSpanPartitioning(
      SpanPartitioning spanPartitioning) {
    this.spanPartitioning = spanPartitioning;
  }

  public SpanAggregation getSpanAggregation() {
    return spanAggregation;
  }

  public void setSpanAggregation(
      SpanAggregation spanAggregation) {
    this.spanAggregation = spanAggregation;
  }

  public TraceStorage getTraceStorage() {
    return traceStorage;
  }

  public void setTraceStorage(
      TraceStorage traceStorage) {
    this.traceStorage = traceStorage;
  }

  public DependencyStorage getDependencyStorage() {
    return dependencyStorage;
  }

  public void setDependencyStorage(
      DependencyStorage dependencyStorage) {
    this.dependencyStorage = dependencyStorage;
  }

  static class SpanPartitioning {
    private Boolean enabled;
    private String spansTopic;
    private Map<String, String> overrides = new LinkedHashMap<>();

    public Boolean getEnabled() {
      return enabled;
    }

    public void setEnabled(Boolean enabled) {
      this.enabled = enabled;
    }

    public String getSpansTopic() {
      return spansTopic;
    }

    public void setSpansTopic(String spansTopic) {
      this.spansTopic = spansTopic;
    }

    public Map<String, String> getOverrides() {
      return overrides;
    }

    public void setOverrides(Map<String, String> overrides) {
      this.overrides = overrides;
    }

    SpanPartitioningBuilder toBuilder() {
      SpanPartitioningBuilder builder = new SpanPartitioningBuilder();
      if (enabled != null) builder.enabled(enabled);
      if (spansTopic != null) builder.spansTopic(spansTopic);
      if (overrides != null) builder.overrides(overrides);
      return builder;
    }
  }

  static class SpanAggregation {
    private Boolean enabled;
    private String spansTopic;
    private String traceTopic;
    private String dependencyTopic;
    private Long traceTimeout;
    private Map<String, String> overrides = new LinkedHashMap<>();

    public Boolean getEnabled() {
      return enabled;
    }

    public void setEnabled(Boolean enabled) {
      this.enabled = enabled;
    }

    public String getSpansTopic() {
      return spansTopic;
    }

    public void setSpansTopic(String spansTopic) {
      this.spansTopic = spansTopic;
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

    public Long getTraceTimeout() {
      return traceTimeout;
    }

    public void setTraceTimeout(Long traceTimeout) {
      this.traceTimeout = traceTimeout;
    }

    public Map<String, String> getOverrides() {
      return overrides;
    }

    public void setOverrides(Map<String, String> overrides) {
      this.overrides = overrides;
    }

    public SpanAggregationBuilder toBuilder() {
      SpanAggregationBuilder builder = new SpanAggregationBuilder();
      if (enabled != null) builder.enabled(enabled);
      if (traceTimeout != null) builder.traceTimeout(Duration.ofMillis(traceTimeout));
      if (spansTopic != null) builder.spansTopic(spansTopic);
      if (traceTopic != null) builder.traceTopic(traceTopic);
      if (dependencyTopic != null) builder.dependencyTopic(dependencyTopic);
      if (overrides != null) builder.overrides(overrides);
      return builder;
    }
  }

  static class TraceStorage {
    private Boolean enabled;
    private String spansTopic;
    private Long ttlCheckInterval;
    private Long ttl;
    private Map<String, String> overrides = new LinkedHashMap<>();

    public Boolean getEnabled() {
      return enabled;
    }

    public void setEnabled(Boolean enabled) {
      this.enabled = enabled;
    }

    public String getSpansTopic() {
      return spansTopic;
    }

    public void setSpansTopic(String spansTopic) {
      this.spansTopic = spansTopic;
    }

    public Long getTtlCheckInterval() {
      return ttlCheckInterval;
    }

    public void setTtlCheckInterval(Long ttlCheckInterval) {
      this.ttlCheckInterval = ttlCheckInterval;
    }

    public Long getTtl() {
      return ttl;
    }

    public void setTtl(Long ttl) {
      this.ttl = ttl;
    }

    public Map<String, String> getOverrides() {
      return overrides;
    }

    public void setOverrides(Map<String, String> overrides) {
      this.overrides = overrides;
    }

    TraceStorageBuilder toBuilder() {
      TraceStorageBuilder builder = new TraceStorageBuilder();
      if (enabled != null) builder.enabled(enabled);
      if (ttlCheckInterval != null) builder.ttlCheckInterval(Duration.ofMillis(ttlCheckInterval));
      if (ttl != null) builder.ttl(Duration.ofMillis(ttl));
      if (spansTopic != null) builder.spansTopic(spansTopic);
      if (overrides != null) builder.overrides(overrides);
      return builder;
    }
  }

  static class DependencyStorage {
    private Boolean enabled;
    private String dependencyTopic;
    private Long ttl;
    private Map<String, String> overrides = new LinkedHashMap<>();

    public Boolean getEnabled() {
      return enabled;
    }

    public void setEnabled(Boolean enabled) {
      this.enabled = enabled;
    }

    public String getDependencyTopic() {
      return dependencyTopic;
    }

    public void setDependencyTopic(String dependencyTopic) {
      this.dependencyTopic = dependencyTopic;
    }

    public Long getTtl() {
      return ttl;
    }

    public void setTtl(Long ttl) {
      this.ttl = ttl;
    }

    public Map<String, String> getOverrides() {
      return overrides;
    }

    public void setOverrides(Map<String, String> overrides) {
      this.overrides = overrides;
    }

    DependencyStorageBuilder toBuilder() {
      DependencyStorageBuilder builder = new DependencyStorageBuilder();
      if (enabled != null) builder.enabled(enabled);
      if (dependencyTopic != null) builder.dependencyTopic(dependencyTopic);
      if (ttl != null) builder.ttl(Duration.ofMillis(ttl));
      if (overrides != null) builder.overrides(overrides);
      return builder;
    }
  }
}
