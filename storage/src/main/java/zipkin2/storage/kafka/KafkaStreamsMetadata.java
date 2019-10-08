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

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

final class KafkaStreamsMetadata {
  static KafkaStreamsMetadata create(
    Collection<org.apache.kafka.streams.state.StreamsMetadata> other) {
    KafkaStreamsMetadata metadata = new KafkaStreamsMetadata();
    metadata.metadata = other.stream().map(StreamsMetadata::create).collect(Collectors.toSet());
    return metadata;
  }

  Set<StreamsMetadata> metadata;

  KafkaStreamsMetadata() {
  }

  public void setMetadata(
    Set<StreamsMetadata> metadata) {
    this.metadata = metadata;
  }

  public Set<StreamsMetadata> getMetadata() {
    return metadata;
  }

  static final class StreamsMetadata {
    static StreamsMetadata create(org.apache.kafka.streams.state.StreamsMetadata other) {
      StreamsMetadata metadata = new StreamsMetadata();
      metadata.hostInfo = HostInfo.create(other.hostInfo());
      metadata.storeNames = other.stateStoreNames();
      metadata.topicPartitions = other.topicPartitions().stream()
        .map(TopicPartition::create)
        .collect(Collectors.toSet());
      return metadata;
    }

    HostInfo hostInfo;
    Set<String> storeNames;
    Set<TopicPartition> topicPartitions;

    StreamsMetadata() {
    }

    public void setHostInfo(HostInfo hostInfo) {
      this.hostInfo = hostInfo;
    }

    public void setStoreNames(Set<String> storeNames) {
      this.storeNames = storeNames;
    }

    public void setTopicPartitions(
      Set<TopicPartition> topicPartitions) {
      this.topicPartitions = topicPartitions;
    }

    public HostInfo getHostInfo() {
      return hostInfo;
    }

    public Set<String> getStoreNames() {
      return storeNames;
    }

    public Set<TopicPartition> getTopicPartitions() {
      return topicPartitions;
    }

    static final class HostInfo {
      static HostInfo create(org.apache.kafka.streams.state.HostInfo other) {
        HostInfo hostInfo = new HostInfo();
        hostInfo.host = other.host();
        hostInfo.port = other.port();
        return hostInfo;
      }

      String host;
      Integer port;

      HostInfo() {
      }

      public void setHost(String host) {
        this.host = host;
      }

      public void setPort(Integer port) {
        this.port = port;
      }

      public String getHost() {
        return host;
      }

      public Integer getPort() {
        return port;
      }
    }

    static final class TopicPartition {
      static TopicPartition create(org.apache.kafka.common.TopicPartition other) {
        TopicPartition topicPartition = new TopicPartition();
        topicPartition.partition = other.partition();
        topicPartition.topic = other.topic();
        return topicPartition;
      }

      TopicPartition() {
      }

      String topic;
      Integer partition;

      public void setTopic(String topic) {
        this.topic = topic;
      }

      public void setPartition(Integer partition) {
        this.partition = partition;
      }

      public String getTopic() {
        return topic;
      }

      public Integer getPartition() {
        return partition;
      }
    }
  }
}
