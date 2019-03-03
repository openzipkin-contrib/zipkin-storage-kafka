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
package zipkin2.storage.kafka.internal;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import no.sysco.middleware.kafka.util.KafkaStreamsTopologyGraphvizPrinter;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.Test;
import zipkin2.Span;
import zipkin2.TestObjects;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.storage.kafka.streams.AggregationTopologySupplier;

import static org.junit.Assert.assertEquals;

public class StreamsSupplierTest {

  @Test
  public void testTracesStream() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    props.put(StreamsConfig.STATE_DIR_CONFIG,
        "target/kafka-streams-test/" + Instant.now().getEpochSecond());
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);

    String topic = "topic";
    AggregationTopologySupplier aggregationTopologySupplier = new AggregationTopologySupplier(
        topic, "traces",
        "services",
        "dependencies");
    Topology topology = aggregationTopologySupplier.get();
    System.out.println(KafkaStreamsTopologyGraphvizPrinter.print(topology));
    TopologyTestDriver driver = new TopologyTestDriver(topology, props);
    ConsumerRecordFactory<String, byte[]> factory = new ConsumerRecordFactory<>(
        topic, new StringSerializer(), new ByteArraySerializer());
    Span root =
        Span.newBuilder().traceId("a").id("a").timestamp(TestObjects.TODAY).duration(10).build();
    byte[] encode = SpanBytesEncoder.PROTO3.encode(root);
    driver.pipeInput(factory.create(topic, "000000000000000a", encode));
    Span child =
        Span.newBuilder().traceId("a").id("b").timestamp(TestObjects.TODAY).duration(2).build();
    byte[] encodedChild = SpanBytesEncoder.PROTO3.encode(child);
    driver.pipeInput(factory.create(topic, "000000000000000a", encodedChild));
    driver.advanceWallClockTime(1000);
    for (Map.Entry<String, StateStore> storeEntry : driver.getAllStateStores().entrySet()) {
      storeEntry.getValue().flush();
      System.out.println(storeEntry.getKey());
    }
    KeyValueStore<String, List<Span>> store = driver.getKeyValueStore("traces");
    List<Span> spans = store.get("000000000000000a");
    assertEquals(2, spans.size());
  }
}