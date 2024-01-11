/*
 * Copyright 2019-2024 The OpenZipkin Authors
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
package zipkin2.storage.kafka.streams;

import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.jupiter.api.Test;
import zipkin2.DependencyLink;
import zipkin2.storage.kafka.streams.serdes.DependencyLinkSerde;

import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.storage.kafka.streams.DependencyStorageTopology.DEPENDENCIES_STORE_NAME;

class DependencyStorageTopologyTest {
  String dependencyTopic = "zipkin-dependency";
  DependencyLinkSerde dependencyLinkSerde = new DependencyLinkSerde();
  Properties props = new Properties();

  DependencyStorageTopologyTest() {
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    props.put(StreamsConfig.STATE_DIR_CONFIG,
      "target/kafka-streams-test/" + System.currentTimeMillis());
  }

  @Test void should_doNothing_whenDisabled() {
    // Given: configs
    Duration dependenciesRetentionPeriod = Duration.ofMinutes(1);
    Duration dependenciesWindowSize = Duration.ofMillis(100);
    // When: topology created
    Topology topology = new DependencyStorageTopology(
      dependencyTopic,
      dependenciesRetentionPeriod,
      dependenciesWindowSize,
      false).get();
    TopologyDescription description = topology.describe();
    // Then: topology with 1 thread
    assertThat(description.subtopologies()).hasSize(0);
    // Given: streams configuration
    TopologyTestDriver testDriver = new TopologyTestDriver(topology, props);
    testDriver.close();
  }

  @Test void should_storeDependencies() {
    // Given: configs
    Duration dependenciesRetentionPeriod = Duration.ofMinutes(1);
    Duration dependenciesWindowSize = Duration.ofMillis(100);
    // When: topology created
    Topology topology = new DependencyStorageTopology(
      dependencyTopic,
      dependenciesRetentionPeriod,
      dependenciesWindowSize,
      true).get();
    TopologyDescription description = topology.describe();
    // Then: topology with 1 thread
    assertThat(description.subtopologies()).hasSize(1);
    // Given: streams configuration
    TopologyTestDriver testDriver = new TopologyTestDriver(topology, props);
    // When: a trace is passed
    TestInputTopic<String, DependencyLink> inputTopic =
      testDriver.createInputTopic(dependencyTopic, new StringSerializer(),
        dependencyLinkSerde.serializer());
    DependencyLink dependencyLink = DependencyLink.newBuilder()
      .parent("svc_a").child("svc_b").callCount(1).errorCount(0)
      .build();
    String dependencyLinkId = "svc_a:svc_b";
    inputTopic.pipeInput(dependencyLinkId, dependencyLink, 10L);
    WindowStore<String, DependencyLink> links = testDriver.getWindowStore(DEPENDENCIES_STORE_NAME);
    // Then: dependency link created
    WindowStoreIterator<DependencyLink> firstLink = links.fetch(dependencyLinkId, 0L, 100L);
    assertThat(firstLink).hasNext();
    assertThat(firstLink.next().value).isEqualTo(dependencyLink);
    // When: new links appear
    inputTopic.pipeInput(dependencyLinkId, dependencyLink, 90L);
    // Then: dependency link increases
    WindowStoreIterator<DependencyLink> secondLink = links.fetch(dependencyLinkId, 0L, 100L);
    assertThat(secondLink).hasNext();
    assertThat(secondLink.next().value.callCount()).isEqualTo(2);
    // When: time moves forward
    testDriver.advanceWallClockTime(dependenciesRetentionPeriod.plusMillis(91L));
    inputTopic.pipeInput(dependencyLinkId, dependencyLink);
    // Then: dependency link is removed and restarted
    KeyValueIterator<Windowed<String>, DependencyLink> thirdLink = links.all();
    assertThat(thirdLink).hasNext();
    assertThat(thirdLink.next().value.callCount()).isEqualTo(1);
    // Close resources
    testDriver.close();
    dependencyLinkSerde.close();
  }
}
