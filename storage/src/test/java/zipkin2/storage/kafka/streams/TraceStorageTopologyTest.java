/*
 * Copyright 2019-2021 The OpenZipkin Authors
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
import org.apache.kafka.streams.KeyValue;
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
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.storage.kafka.streams.serdes.SpansSerde;

import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.storage.kafka.streams.TraceStorageTopology.AUTOCOMPLETE_TAGS_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStorageTopology.SPAN_NAMES_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStorageTopology.TRACES_STORE_NAME;

class TraceStorageTopologyTest {
  String spansTopic = "zipkin-spans";
  Properties props = new Properties();

  TraceStorageTopologyTest() {
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
    // When: topology provided
    Topology topology = new TraceStorageTopology(
      spansTopic,
      autocompleteKeys,
      traceTtl,
      traceTtlCheckInterval,
      0,
      false,
      false).get();
    TopologyDescription description = topology.describe();
    // Then:
    assertThat(description.subtopologies()).isEmpty();
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
    Topology topology = new TraceStorageTopology(
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
    TestInputTopic<String, List<Span>> factory =
      testDriver.createInputTopic(spansTopic, new StringSerializer(), spansSerde.serializer());
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
    factory.pipeInput(a.traceId(), spans, 10L);
    // Then: trace stores are filled
    WindowStore<String, List<Span>> traces = testDriver.getWindowStore(TRACES_STORE_NAME);
    try (final WindowStoreIterator<List<Span>> fetch = traces.fetch(a.traceId(), 0, 10000L)) {
      assertThat(fetch.hasNext()).isTrue();
      final KeyValue<Long, List<Span>> next = fetch.next();
      assertThat(next.value).isEqualTo(spans);
    }
    // Then: service name stores are filled
    WindowStore<String, Set<String>> spanNames = testDriver.getWindowStore(SPAN_NAMES_STORE_NAME);
    assertThat(spanNames).isNull();
    WindowStore<String, Set<String>> tags = testDriver.getWindowStore(AUTOCOMPLETE_TAGS_STORE_NAME);
    assertThat(tags).isNull();
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
    Topology topology = new TraceStorageTopology(
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
    TestInputTopic<String, List<Span>> factory =
      testDriver.createInputTopic(spansTopic, new StringSerializer(), spansSerde.serializer());
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
    factory.pipeInput(a.traceId(), spans, 10L);
    // Then: trace stores are filled
    WindowStore<String, List<Span>> traces = testDriver.getWindowStore(TRACES_STORE_NAME);
    try (final WindowStoreIterator<List<Span>> fetch = traces.fetch(a.traceId(), 0, 10000L)) {
      assertThat(fetch).hasNext();
      assertThat(fetch.next())
        .extracting(next -> next.value)
        .isEqualTo(spans);
      assertThat(fetch).isExhausted();
    }
    WindowStore<String, Set<String>> spanNames = testDriver.getWindowStore(SPAN_NAMES_STORE_NAME);
    try (
      final KeyValueIterator<Windowed<String>, Set<String>> fetch = spanNames.fetchAll(0, 10000L)) {
      assertThat(fetch).hasNext();
      assertThat(fetch.next())
        .extracting(next -> next.key.key(), next -> next.value)
        .containsExactly("svc_a", Collections.singleton("op_a"));
      assertThat(fetch).hasNext();
      assertThat(fetch.next())
        .extracting(next -> next.key.key(), next -> next.value)
        .containsExactly("svc_b", Collections.singleton("op_b"));
      assertThat(fetch).isExhausted();
    }
    WindowStore<String, Set<String>> tagsStore = testDriver.getWindowStore(AUTOCOMPLETE_TAGS_STORE_NAME);
    try (
      final KeyValueIterator<Windowed<String>, Set<String>> fetch = tagsStore.fetchAll(0, 10010L)) {
      assertThat(fetch).hasNext();
      assertThat(fetch.next())
        .extracting(next -> next.key.key(), next -> next.value)
        .containsExactly("environment", Collections.singleton("dev"));
      assertThat(fetch).isExhausted();
    }
    // Finally close resources
    testDriver.close();
    spansSerde.close();
  }
}
