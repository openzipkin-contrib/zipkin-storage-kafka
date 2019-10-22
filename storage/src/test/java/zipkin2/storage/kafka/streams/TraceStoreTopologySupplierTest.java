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
package zipkin2.storage.kafka.streams;

import java.time.Duration;
import java.util.ArrayList;
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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.storage.kafka.streams.TraceStoreTopologySupplier.AUTOCOMPLETE_TAGS_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStoreTopologySupplier.SERVICE_NAMES_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStoreTopologySupplier.SPAN_IDS_BY_TS_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStoreTopologySupplier.SPAN_NAMES_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStoreTopologySupplier.TRACES_STORE_NAME;

class TraceStoreTopologySupplierTest {
  String spansTopic = "zipkin-spans";
  Properties props = new Properties();

  TraceStoreTopologySupplierTest() {
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    props.put(StreamsConfig.STATE_DIR_CONFIG,
        "target/kafka-streams-test/" + System.currentTimeMillis());
  }

  @Test void should_doNothing_whenAllDisabled() {
    // Given: configs
    Duration traceTtl = Duration.ofMillis(5);
    Duration traceTtlCheckInterval = Duration.ofMinutes(1);
    List<String> autocompleteKeys = Collections.singletonList("environment");
    SpansSerde spansSerde = new SpansSerde();
    // When: topology provided
    Topology topology = new TraceStoreTopologySupplier(
        spansTopic,
        autocompleteKeys,
        traceTtl,
        traceTtlCheckInterval,
        0,
        false,
        false).get();
    TopologyDescription description = topology.describe();
    // Then:
    assertThat(description.subtopologies()).hasSize(0);
    // Given: streams config
    TopologyTestDriver testDriver = new TopologyTestDriver(topology, props);
    testDriver.close();
  }

  @Test void should_persistSpans_and_onlyQueryTraces_whenEnabled() {
    // Given: configs
    Duration traceTtl = Duration.ofMillis(5);
    Duration traceTtlCheckInterval = Duration.ofMinutes(1);
    List<String> autocompleteKeys = Collections.singletonList("environment");
    SpansSerde spansSerde = new SpansSerde();
    // When: topology provided
    Topology topology = new TraceStoreTopologySupplier(
        spansTopic,
        autocompleteKeys,
        traceTtl,
        traceTtlCheckInterval,
        0,
        true,
        false).get();
    TopologyDescription description = topology.describe();
    // Then: 1 thread prepared
    assertThat(description.subtopologies()).hasSize(1);
    // Given: streams config
    TopologyTestDriver testDriver = new TopologyTestDriver(topology, props);
    // When: a trace is passed
    ConsumerRecordFactory<String, List<Span>> factory =
        new ConsumerRecordFactory<>(spansTopic, new StringSerializer(), spansSerde.serializer());
    Span a = Span.newBuilder().traceId("a").id("a").name("op_a").kind(Span.Kind.CLIENT)
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_a").build())
        .timestamp(10000L).duration(11L)
        .putTag("environment", "dev")
        .build();
    Span b = Span.newBuilder().traceId("a").id("b").name("op_b").kind(Span.Kind.SERVER)
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_b").build())
        .timestamp(10000L).duration(10L)
        .build();
    Span c = Span.newBuilder().traceId("c").id("c").name("op_a").kind(Span.Kind.CLIENT)
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_a").build())
        .timestamp(10000L).duration(11L)
        .putTag("environment", "dev")
        .build();
    List<Span> spans = Arrays.asList(a, b, c);
    testDriver.pipeInput(factory.create(spansTopic, a.traceId(), spans, 10L));
    // Then: trace stores are filled
    KeyValueStore<String, List<Span>> traces = testDriver.getKeyValueStore(TRACES_STORE_NAME);
    assertThat(traces.get(a.traceId())).containsExactlyElementsOf(spans);
    KeyValueStore<Long, Set<String>> spanIdsByTs =
        testDriver.getKeyValueStore(SPAN_IDS_BY_TS_STORE_NAME);
    KeyValueIterator<Long, Set<String>> ids = spanIdsByTs.all();
    assertThat(ids).hasNext();
    assertThat(ids.next().value).containsExactly(a.traceId());
    // Then: service name stores are filled
    KeyValueStore<String, String> serviceNames =
        testDriver.getKeyValueStore(SERVICE_NAMES_STORE_NAME);
    assertThat(serviceNames).isNull();
    KeyValueStore<String, Set<String>> spanNames =
        testDriver.getKeyValueStore(SPAN_NAMES_STORE_NAME);
    assertThat(spanNames).isNull();
    KeyValueStore<String, Set<String>> autocompleteTags =
        testDriver.getKeyValueStore(AUTOCOMPLETE_TAGS_STORE_NAME);
    assertThat(autocompleteTags).isNull();
    // Finally close resources
    testDriver.close();
    spansSerde.close();
  }

  @Test void should_persistSpans_and_searchQueryTraces_whenAllEnabled() {
    // Given: configs
    Duration traceTtl = Duration.ofMillis(5);
    Duration traceTtlCheckInterval = Duration.ofMinutes(1);
    List<String> autocompleteKeys = Collections.singletonList("environment");
    SpansSerde spansSerde = new SpansSerde();
    // When: topology provided
    Topology topology = new TraceStoreTopologySupplier(
        spansTopic,
        autocompleteKeys,
        traceTtl,
        traceTtlCheckInterval,
        0,
        true,
        true).get();
    TopologyDescription description = topology.describe();
    // Then: 1 thread prepared
    assertThat(description.subtopologies()).hasSize(1);
    // Given: streams config
    TopologyTestDriver testDriver = new TopologyTestDriver(topology, props);
    // When: a trace is passed
    ConsumerRecordFactory<String, List<Span>> factory =
        new ConsumerRecordFactory<>(spansTopic, new StringSerializer(), spansSerde.serializer());
    Span a = Span.newBuilder().traceId("a").id("a").name("op_a").kind(Span.Kind.CLIENT)
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_a").build())
        .timestamp(10000L).duration(11L)
        .putTag("environment", "dev")
        .build();
    Span b = Span.newBuilder().traceId("a").id("b").name("op_b").kind(Span.Kind.SERVER)
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_b").build())
        .timestamp(10000L).duration(10L)
        .build();
    Span c = Span.newBuilder().traceId("c").id("c").name("op_a").kind(Span.Kind.CLIENT)
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_a").build())
        .timestamp(10000L).duration(11L)
        .putTag("environment", "dev")
        .build();
    List<Span> spans = Arrays.asList(a, b, c);
    testDriver.pipeInput(factory.create(spansTopic, a.traceId(), spans, 10L));
    // Then: trace stores are filled
    KeyValueStore<String, List<Span>> traces = testDriver.getKeyValueStore(TRACES_STORE_NAME);
    assertThat(traces.get(a.traceId())).containsExactlyElementsOf(spans);
    KeyValueStore<Long, Set<String>> spanIdsByTs =
        testDriver.getKeyValueStore(SPAN_IDS_BY_TS_STORE_NAME);
    KeyValueIterator<Long, Set<String>> ids = spanIdsByTs.all();
    assertThat(ids).hasNext();
    assertThat(ids.next().value).containsExactly(a.traceId());
    // Then: service name stores are filled
    KeyValueStore<String, String> serviceNames =
        testDriver.getKeyValueStore(SERVICE_NAMES_STORE_NAME);
    List<String> serviceNameList = new ArrayList<>();
    serviceNames.all().forEachRemaining(serviceName -> serviceNameList.add(serviceName.value));
    assertThat(serviceNameList).hasSize(2);
    assertThat(serviceNames.get("svc_a")).isEqualTo("svc_a");
    assertThat(serviceNames.get("svc_b")).isEqualTo("svc_b");
    KeyValueStore<String, Set<String>> spanNames =
        testDriver.getKeyValueStore(SPAN_NAMES_STORE_NAME);
    assertThat(spanNames.get("svc_a")).containsExactly("op_a");
    assertThat(spanNames.get("svc_b")).containsExactly("op_b");
    KeyValueStore<String, Set<String>> autocompleteTags =
        testDriver.getKeyValueStore(AUTOCOMPLETE_TAGS_STORE_NAME);
    assertThat(autocompleteTags.get("environment")).containsExactly("dev");
    // When: clock moves forward
    Span d = Span.newBuilder()
        .traceId("d")
        .id("d")
        .timestamp(
            MILLISECONDS.toMicros(traceTtlCheckInterval.toMillis()) + MILLISECONDS.toMicros(20))
        .build();
    testDriver.pipeInput(
        factory.create(spansTopic, d.traceId(), Collections.singletonList(d),
            traceTtlCheckInterval.plusMillis(1).toMillis()));
    // Then: Traces store is empty
    assertThat(traces.get(a.traceId())).isNull();
    // Finally close resources
    testDriver.close();
    spansSerde.close();
  }
}
