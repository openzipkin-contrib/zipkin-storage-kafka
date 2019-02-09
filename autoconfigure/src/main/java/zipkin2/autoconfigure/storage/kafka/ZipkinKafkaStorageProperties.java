/*
 * Copyright 2019 [name of copyright owner]
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

import org.springframework.boot.context.properties.ConfigurationProperties;
import zipkin2.storage.kafka.KafkaStorage;

import java.io.Serializable;

@ConfigurationProperties("zipkin.storage.kafka")
public class ZipkinKafkaStorageProperties implements Serializable {
  private static final long serialVersionUID = 0L;

  private boolean ensureTopics = true;
  private String bootstrapServers = "localhost:29092";

  private String spansTopic = "zipkin-spans_v1";
  private String tracesTopic = "zipkin-traces_v1";
  private String servicesTopic = "zipkin-services_v1";
  private String dependenciesTopic = "zipkin-dependencies_v1";

  private String processStateStoreDirectory = "/tmp/zipkin/kafka-streams/process";
  private String indexStateStoreDirectory = "/tmp/zipkin/kafka-streams/index";
  private String indexStorageDirectory = "/tmp/zipkin/index";

  KafkaStorage.Builder toBuilder() {
    return KafkaStorage.newBuilder()
        .ensureTopics(ensureTopics)
        .bootstrapServers(bootstrapServers)
        .spansTopic(KafkaStorage.Topic.builder(spansTopic).build())
        .tracesTopic(KafkaStorage.Topic.builder(tracesTopic).build())
        .servicesTopic(KafkaStorage.Topic.builder(servicesTopic).build())
        .dependenciesTopic(KafkaStorage.Topic.builder(dependenciesTopic).build())
        .processStreamStoreDirectory(processStateStoreDirectory)
        .indexStorageDirectory(indexStorageDirectory);
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

  public String getServicesTopic() {
    return servicesTopic;
  }

  public void setServicesTopic(String servicesTopic) {
    this.servicesTopic = servicesTopic;
  }

  public String getDependenciesTopic() {
    return dependenciesTopic;
  }

  public void setDependenciesTopic(String dependenciesTopic) {
    this.dependenciesTopic = dependenciesTopic;
  }

  public String getProcessStateStoreDirectory() {
    return processStateStoreDirectory;
  }

  public void setProcessStateStoreDirectory(String processStateStoreDirectory) {
    this.processStateStoreDirectory = processStateStoreDirectory;
  }

  public String getIndexStateStoreDirectory() {
    return indexStateStoreDirectory;
  }

  public void setIndexStateStoreDirectory(String indexStateStoreDirectory) {
    this.indexStateStoreDirectory = indexStateStoreDirectory;
  }

  public String getIndexStorageDirectory() {
    return indexStorageDirectory;
  }

  public void setIndexStorageDirectory(String indexStorageDirectory) {
    this.indexStorageDirectory = indexStorageDirectory;
  }
}
