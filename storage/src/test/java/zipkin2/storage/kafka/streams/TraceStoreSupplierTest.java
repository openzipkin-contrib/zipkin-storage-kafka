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
package zipkin2.storage.kafka.streams;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.jupiter.api.Test;
import zipkin2.DependencyLink;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.storage.kafka.streams.serdes.DependencyLinkSerde;
import zipkin2.storage.kafka.streams.serdes.SpansSerde;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static zipkin2.storage.kafka.streams.TraceStoreSupplier.DEPENDENCY_LINKS_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStoreSupplier.SERVICE_NAMES_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStoreSupplier.SPAN_NAMES_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStoreSupplier.SPAN_IDS_BY_TS_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStoreSupplier.TRACES_STORE_NAME;

class TraceStoreSupplierTest {

  @Test void should_persist_stores() {
    // Given
    String tracesTopicName = "traces";
    String dependencyLinksTopicName = "dependency-links";

    Duration tracesRetentionScanFrequency = Duration.ofMinutes(1);
    Duration tracesRetentionPeriod = Duration.ofMillis(5);
    Duration dependenciesRetentionPeriod = Duration.ofMinutes(1);
    Duration dependenciesWindowSize = Duration.ofMillis(100);
    Topology topology = new TraceStoreSupplier(
        tracesTopicName,
        dependencyLinksTopicName,
        tracesRetentionScanFrequency,
        tracesRetentionPeriod,
        dependenciesRetentionPeriod,
        dependenciesWindowSize).get();

    TopologyDescription description = topology.describe();
    System.out.println("Topology: \n" + description);

    assertEquals(2, description.subtopologies().size());

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    props.put(StreamsConfig.STATE_DIR_CONFIG,
        "target/kafka-streams-test/" + System.currentTimeMillis());
    TopologyTestDriver testDriver = new TopologyTestDriver(topology, props);

    SpansSerde spansSerde = new SpansSerde();

    // When: a trace is passed
    ConsumerRecordFactory<String, List<Span>> factory =
        new ConsumerRecordFactory<>(tracesTopicName, new StringSerializer(),
            spansSerde.serializer());
    Span a = Span.newBuilder().traceId("a").id("a").name("op_a").kind(Span.Kind.CLIENT)
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_a").build())
        .timestamp(10000L).duration(11L)
        .build();
    Span b = Span.newBuilder().traceId("a").id("b").name("op_b").kind(Span.Kind.SERVER)
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_b").build())
        .timestamp(10000L).duration(10L)
        .build();
    List<Span> spans = Arrays.asList(a, b);
    testDriver.pipeInput(factory.create(tracesTopicName, a.traceId(), spans, 10L));

    // Then: trace stores are filled
    KeyValueStore<String, List<Span>> traces =
        testDriver.getKeyValueStore(TRACES_STORE_NAME);
    assertEquals(traces.get(a.traceId()), spans);
    KeyValueStore<Long, Set<String>> spanIdsByTs =
        testDriver.getKeyValueStore(SPAN_IDS_BY_TS_STORE_NAME);
    KeyValueIterator<Long, Set<String>> ids = spanIdsByTs.all();
    assertTrue(ids.hasNext());
    assertEquals(ids.next().value, Collections.singleton(a.traceId()));

    // Then: service name stores are filled
    KeyValueStore<String, String> serviceNames =
        testDriver.getKeyValueStore(SERVICE_NAMES_STORE_NAME);
    assertEquals("svc_a", serviceNames.get("svc_a"));
    assertEquals("svc_b", serviceNames.get("svc_b"));
    KeyValueStore<String, Set<String>> spanNames =
        testDriver.getKeyValueStore(SPAN_NAMES_STORE_NAME);
    assertEquals(Collections.singleton("op_a"), spanNames.get("svc_a"));
    assertEquals(Collections.singleton("op_b"), spanNames.get("svc_b"));

    // When: clock moves forward
    Span c = Span.newBuilder()
        .traceId("c")
        .id("c")
        .timestamp(tracesRetentionScanFrequency.toMillis() * 1000 + 20000L)
        .build();
    testDriver.pipeInput(
        factory.create(tracesTopicName, c.traceId(), Collections.singletonList(c),
            tracesRetentionScanFrequency.toMillis() + 1));

    // Then: Traces store is empty
    assertNull(traces.get(a.traceId()));
  }

  @Test void should_store_dependencies() {
    // Given
    String tracesTopicName = "traces";
    String dependencyLinksTopicName = "dependency-links";

    Duration tracesRetentionScanFrequency = Duration.ofMinutes(1);
    Duration tracesRetentionPeriod = Duration.ofMillis(5);
    Duration dependenciesRetentionPeriod = Duration.ofMinutes(1);
    Duration dependenciesWindowSize = Duration.ofMillis(100);
    Topology topology = new TraceStoreSupplier(
        tracesTopicName,
        dependencyLinksTopicName,
        tracesRetentionScanFrequency,
        tracesRetentionPeriod,
        dependenciesRetentionPeriod,
        dependenciesWindowSize).get();

    TopologyDescription description = topology.describe();
    System.out.println("Topology: \n" + description);

    assertEquals(2, description.subtopologies().size());

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
    testDriver.advanceWallClockTime(dependenciesRetentionPeriod.toMillis() + 91L);
    testDriver.pipeInput(
        factory.create(dependencyLinksTopicName, dependencyLinkId, dependencyLink));

    // Then: dependency link is removed and restarted
    KeyValueIterator<Windowed<String>, DependencyLink> fetch3 = links.all();
    assertTrue(fetch3.hasNext());
    assertEquals(fetch3.next().value.callCount(), 1);
  }
}