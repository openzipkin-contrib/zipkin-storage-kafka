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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.Test;
import zipkin2.DependencyLink;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.storage.kafka.streams.serdes.DependencyLinkSerde;
import zipkin2.storage.kafka.streams.serdes.SpansSerde;

import static org.assertj.core.api.Assertions.assertThat;

class AggregationTopologySupplierTest {
  String spansTopic = "spans";
  String traceTopic = "traces";
  String dependencyTopic = "dependencies";

  Properties props = new Properties();

  AggregationTopologySupplierTest() {
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
  }

  @Test void should_doNothing_whenAggregationDisabled() {
    Duration traceTimeout = Duration.ofSeconds(1);
    Topology topology = new AggregationTopologySupplier(
        spansTopic,
        traceTopic,
        dependencyTopic,
        traceTimeout,
        false).get();
    TopologyDescription description = topology.describe();
    // Then: single threaded topology
    assertThat(description.subtopologies()).hasSize(0);
    TopologyTestDriver testDriver = new TopologyTestDriver(topology, props);
    testDriver.close();
  }

  @Test void should_aggregateSpans_and_mapDependencies() {
    // Given: configuration
    Duration traceTimeout = Duration.ofSeconds(1);
    SpansSerde spansSerde = new SpansSerde();
    DependencyLinkSerde dependencyLinkSerde = new DependencyLinkSerde();
    // When: topology built
    Topology topology = new AggregationTopologySupplier(
        spansTopic,
        traceTopic,
        dependencyTopic,
        traceTimeout,
        true).get();
    TopologyDescription description = topology.describe();
    // Then: single threaded topology
    assertThat(description.subtopologies()).hasSize(1);
    // Given: test driver
    TopologyTestDriver testDriver = new TopologyTestDriver(topology, props);
    // When: two related spans coming on the same Session window
    ConsumerRecordFactory<String, List<Span>> factory =
        new ConsumerRecordFactory<>(spansTopic, new StringSerializer(), spansSerde.serializer());
    Span a = Span.newBuilder().traceId("a").id("a").name("op_a").kind(Span.Kind.CLIENT)
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_a").build())
        .build();
    Span b = Span.newBuilder().traceId("a").id("b").name("op_b").kind(Span.Kind.SERVER)
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_b").build())
        .build();
    testDriver.pipeInput(
        factory.create(spansTopic, a.traceId(), Collections.singletonList(a), 0L));
    testDriver.pipeInput(
        factory.create(spansTopic, b.traceId(), Collections.singletonList(b), 0L));
    // When: and new record arrive, moving the event clock further than inactivity gap
    Span c = Span.newBuilder().traceId("c").id("c").build();
    testDriver.pipeInput(factory.create(spansTopic, c.traceId(), Collections.singletonList(c),
        traceTimeout.toMillis() + 1));
    // Then: a trace is aggregated.1
    ProducerRecord<String, List<Span>> trace =
        testDriver.readOutput(traceTopic, new StringDeserializer(), spansSerde.deserializer());
    assertThat(trace).isNotNull();
    OutputVerifier.compareKeyValue(trace, a.traceId(), Arrays.asList(a, b));
    // Then: a dependency link is created
    ProducerRecord<String, DependencyLink> linkRecord =
        testDriver.readOutput(dependencyTopic, new StringDeserializer(),
            dependencyLinkSerde.deserializer());
    assertThat(linkRecord).isNotNull();
    DependencyLink link = DependencyLink.newBuilder()
        .parent("svc_a").child("svc_b").callCount(1).errorCount(0)
        .build();
    OutputVerifier.compareKeyValue(linkRecord, "svc_a:svc_b", link);
    //Finally close resources
    testDriver.close();
    spansSerde.close();
    dependencyLinkSerde.close();
  }
}
