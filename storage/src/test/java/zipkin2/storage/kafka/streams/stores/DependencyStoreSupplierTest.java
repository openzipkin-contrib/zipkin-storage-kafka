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
package zipkin2.storage.kafka.streams.stores;

import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.jupiter.api.Test;
import zipkin2.DependencyLink;
import zipkin2.storage.kafka.streams.serdes.DependencyLinkSerde;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static zipkin2.storage.kafka.streams.stores.DependencyStoreSupplier.DEPENDENCY_LINKS_STORE_NAME;

class DependencyStoreSupplierTest {

  @Test void should_store_dependencies() {
    // Given
    String dependencyLinksTopicName = "dependency-links";

    Duration retentionPeriod = Duration.ofMinutes(1);
    Duration windowSize = Duration.ofMillis(100);
    Topology topology =
        new DependencyStoreSupplier(dependencyLinksTopicName, retentionPeriod, windowSize).get();

    TopologyDescription description = topology.describe();
    System.out.println("Topology: \n" + description);

    assertEquals(1, description.subtopologies().size());

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    props.put(StreamsConfig.STATE_DIR_CONFIG,
        "target/kafka-streams-test/" + System.currentTimeMillis());
    TopologyTestDriver testDriver = new TopologyTestDriver(topology, props);

    DependencyLinkSerde dependencyLinkSerde = new DependencyLinkSerde();

    // When: a trace is passed
    ConsumerRecordFactory<String, DependencyLink> factory =
        new ConsumerRecordFactory<>(dependencyLinksTopicName, new StringSerializer(),
            dependencyLinkSerde.serializer());
    DependencyLink dependencyLink = DependencyLink.newBuilder()
        .parent("svc_a").child("svc_b").callCount(1).errorCount(0)
        .build();
    String dependencyLinkId = "svc_a:svc_b";
    testDriver.pipeInput(
        factory.create(dependencyLinksTopicName, dependencyLinkId, dependencyLink, 10L));

    WindowStore<String, DependencyLink> links =
        testDriver.getWindowStore(DEPENDENCY_LINKS_STORE_NAME);

    // Then: dependency link created
    WindowStoreIterator<DependencyLink> fetch1 = links.fetch(dependencyLinkId, 0L, 100L);
    assertTrue(fetch1.hasNext());
    assertEquals(fetch1.next().value, dependencyLink);

    // When: new links appear
    testDriver.pipeInput(
        factory.create(dependencyLinksTopicName, dependencyLinkId, dependencyLink, 90L));

    // Then: dependency link increases
    WindowStoreIterator<DependencyLink> fetch2 = links.fetch(dependencyLinkId, 0L, 100L);
    assertTrue(fetch2.hasNext());
    assertEquals(fetch2.next().value.callCount(), 2);

    // When: time moves forward
    testDriver.advanceWallClockTime(retentionPeriod.toMillis() + 91L);
    testDriver.pipeInput(
        factory.create(dependencyLinksTopicName, dependencyLinkId, dependencyLink));

    // Then: dependency link is removed and restarted
    KeyValueIterator<Windowed<String>, DependencyLink> fetch3 = links.all();
    assertTrue(fetch3.hasNext());
    assertEquals(fetch3.next().value.callCount(), 1);
  }
}