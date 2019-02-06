package zipkin2.storage.kafka;

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
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.codec.SpanBytesEncoder;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static zipkin2.TestObjects.TODAY;

public class StreamsSupplierTest {

    @Test
    public void testTracesStream() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "target/kafka-streams-test/" + Instant.now().getEpochSecond());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);

        TopologySupplier topologySupplier = new TopologySupplier(
                "traces", "services", "dependencies");
        Topology topology = topologySupplier.get();
        System.out.println(KafkaStreamsTopologyGraphvizPrinter.print(topology));
        TopologyTestDriver driver = new TopologyTestDriver(topology, props);
        ConsumerRecordFactory<String, byte[]> factory = new ConsumerRecordFactory<>(
                KafkaSpanConsumer.TOPIC, new StringSerializer(), new ByteArraySerializer());
        Span root = Span.newBuilder().traceId("a").id("a").timestamp(TODAY).duration(10).build();
        Span child = Span.newBuilder().traceId("a").id("b").timestamp(TODAY).duration(2).build();
        byte[] encode = SpanBytesEncoder.PROTO3.encodeList(Arrays.asList(root, child));
        driver.pipeInput(factory.create(KafkaSpanConsumer.TOPIC, "", encode));
        driver.advanceWallClockTime(1000);
        for (Map.Entry<String, StateStore> storeEntry : driver.getAllStateStores().entrySet()) {
            storeEntry.getValue().flush();
            System.out.println(storeEntry.getKey());
        }
        KeyValueStore<String, byte[]> store = driver.getKeyValueStore("traces");
        byte[] traceEncoded = store.get("000000000000000a");
        List<Span> spans = SpanBytesDecoder.PROTO3.decodeList(traceEncoded);
        assertEquals(2, spans.size());
    }

}