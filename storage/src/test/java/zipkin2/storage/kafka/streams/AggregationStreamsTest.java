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

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import no.sysco.middleware.kafka.util.KafkaStreamsTopologyGraphvizPrinter;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.Test;
import zipkin2.DependencyLink;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.TestObjects;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.storage.kafka.streams.serdes.DependencyLinkSerde;
import zipkin2.storage.kafka.streams.serdes.SpanNamesSerde;

import static org.junit.Assert.assertEquals;

public class AggregationStreamsTest {
  @Test
  public void shouldAggregate() {
    // given
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    props.put(StreamsConfig.STATE_DIR_CONFIG,
        "target/kafka-streams-test/" + Instant.now().getEpochSecond());
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);

    String spansTopic = "topic";
    String tracesTopic = "traces";
    String servicesTopic = "services";
    String dependenciesTopic = "dependencies";

    AggregationTopologySupplier processTopologySupplier = new AggregationTopologySupplier(
        spansTopic, tracesTopic, servicesTopic, dependenciesTopic);
    Topology topology = processTopologySupplier.get();
    System.out.println(KafkaStreamsTopologyGraphvizPrinter.print(topology));

    // when
    TopologyTestDriver driver = new TopologyTestDriver(topology, props);
    ConsumerRecordFactory<String, byte[]> factory = new ConsumerRecordFactory<>(
        spansTopic, new StringSerializer(), new ByteArraySerializer());
    Span span =
        Span.newBuilder().traceId("a").id("a").timestamp(TestObjects.TODAY).duration(10)
            .localEndpoint(Endpoint.newBuilder().serviceName("svc_a").build())
            .name("op_a").build();
    byte[] encode = SpanBytesEncoder.PROTO3.encode(span);
    driver.pipeInput(factory.create(spansTopic, span.traceId(), encode));
    Span child =
        Span.newBuilder().traceId("a").id("b").timestamp(TestObjects.TODAY).duration(2)
            .localEndpoint(Endpoint.newBuilder().serviceName("svc_a").build())
            .name("op_b").kind(Span.Kind.CLIENT).build();
    byte[] encodedChild = SpanBytesEncoder.PROTO3.encode(child);
    driver.pipeInput(factory.create(spansTopic, span.traceId(), encodedChild));

    Span serverSpan =
        Span.newBuilder().traceId("a").parentId("b").id("c").timestamp(TestObjects.TODAY).duration(2)
            .localEndpoint(Endpoint.newBuilder().serviceName("svc_b").build())
            .name("op_b").kind(Span.Kind.SERVER).build();
    byte[] encodedServerSpan = SpanBytesEncoder.PROTO3.encode(serverSpan);
    driver.pipeInput(factory.create(spansTopic, span.traceId(), encodedServerSpan));

    for (Map.Entry<String, StateStore> storeEntry : driver.getAllStateStores().entrySet()) {
      storeEntry.getValue().flush();
      System.out.println(storeEntry.getKey());
    }

    // then
    ProducerRecord<byte[], byte[]> traceRecord = driver.readOutput(tracesTopic);
    assertEquals(span.traceId(), new String(traceRecord.key()));
    List<Span> spans = SpanBytesDecoder.PROTO3.decodeList(traceRecord.value());
    assertEquals(1, spans.size());

    traceRecord = driver.readOutput(tracesTopic);
    assertEquals(span.traceId(), new String(traceRecord.key()));
    spans = SpanBytesDecoder.PROTO3.decodeList(traceRecord.value());
    assertEquals(2, spans.size());

    traceRecord = driver.readOutput(tracesTopic);
    assertEquals(span.traceId(), new String(traceRecord.key()));
    spans = SpanBytesDecoder.PROTO3.decodeList(traceRecord.value());
    assertEquals(3, spans.size());

    ProducerRecord<String, Set<String>> serviceRecord =
        driver.readOutput(servicesTopic, new StringDeserializer(),
            new SpanNamesSerde.SpanNamesDeserializer());
    assertEquals(span.localServiceName(), serviceRecord.key());
    Set<String> spanNames = serviceRecord.value();
    assertEquals(1, spanNames.size());

    serviceRecord =
        driver.readOutput(servicesTopic, new StringDeserializer(),
            new SpanNamesSerde.SpanNamesDeserializer());
    assertEquals(span.localServiceName(), serviceRecord.key());
    spanNames = serviceRecord.value();
    assertEquals(2, spanNames.size());

    driver.advanceWallClockTime(10000);
    ProducerRecord<String, DependencyLink> depRecord =
        driver.readOutput(dependenciesTopic, new StringDeserializer(),
            new DependencyLinkSerde.DependencyLinkDeserializer());
    assertEquals("svc_a|svc_b", depRecord.key());
    DependencyLink link = depRecord.value();
    assertEquals(1, link.callCount());
  }
}
