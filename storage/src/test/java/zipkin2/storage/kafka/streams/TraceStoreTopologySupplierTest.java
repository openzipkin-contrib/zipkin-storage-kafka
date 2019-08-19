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
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.jupiter.api.Test;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.storage.kafka.streams.serdes.SpansSerde;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static zipkin2.storage.kafka.streams.TraceStoreTopologySupplier.AUTOCOMPLETE_TAGS_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStoreTopologySupplier.SERVICE_NAMES_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStoreTopologySupplier.SPAN_IDS_BY_TS_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStoreTopologySupplier.SPAN_NAMES_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStoreTopologySupplier.TRACES_STORE_NAME;

class TraceStoreTopologySupplierTest {

  @Test void should_persist_stores() {
    // Given: configs
    String spanTopicName = "zipkin-span";
    Duration tracesRetentionScanFrequency = Duration.ofMinutes(1);
    Duration tracesRetentionPeriod = Duration.ofMillis(5);
    List<String> autocompleteKeys = Collections.singletonList("environment");
    SpansSerde spansSerde = new SpansSerde();
    // When: topology provided
    Topology topology = new TraceStoreTopologySupplier(
        spanTopicName,
        autocompleteKeys,
        tracesRetentionScanFrequency,
        tracesRetentionPeriod
    ).get();
    TopologyDescription description = topology.describe();
    System.out.println("Topology: \n" + description);
    // Then: 2 threads prepared
    assertEquals(1, description.subtopologies().size());
    // Given: streams config
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    props.put(StreamsConfig.STATE_DIR_CONFIG,
        "target/kafka-streams-test/" + System.currentTimeMillis());
    TopologyTestDriver testDriver = new TopologyTestDriver(topology, props);
    // When: a trace is passed
    ConsumerRecordFactory<String, List<Span>> factory =
        new ConsumerRecordFactory<>(spanTopicName, new StringSerializer(),
            spansSerde.serializer());
    Span a = Span.newBuilder().traceId("a").id("a").name("op_a").kind(Span.Kind.CLIENT)
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_a").build())
        .timestamp(10000L).duration(11L)
        .putTag("environment", "dev")
        .build();
    Span b = Span.newBuilder().traceId("a").id("b").name("op_b").kind(Span.Kind.SERVER)
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_b").build())
        .timestamp(10000L).duration(10L)
        .build();
    List<Span> spans = Arrays.asList(a, b);
    testDriver.pipeInput(factory.create(spanTopicName, a.traceId(), spans, 10L));
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
    KeyValueStore<String, Set<String>> autocompleteTags =
        testDriver.getKeyValueStore(AUTOCOMPLETE_TAGS_STORE_NAME);
    assertEquals(Collections.singleton("dev"), autocompleteTags.get("environment"));
    // When: clock moves forward
    Span c = Span.newBuilder()
        .traceId("c").id("c")
        .timestamp(tracesRetentionScanFrequency.toMillis() * 1000 + 20000L)
        .build();
    testDriver.pipeInput(
        factory.create(spanTopicName, c.traceId(), Collections.singletonList(c),
            tracesRetentionScanFrequency.toMillis() + 1));

    // Then: Traces store is empty
    assertNull(traces.get(a.traceId()));
  }
}