package zipkin2.storage.kafka.streams.aggregation;

import java.time.Duration;
import java.util.Arrays;
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
import zipkin2.Span;
import zipkin2.storage.kafka.streams.serdes.SpanSerde;
import zipkin2.storage.kafka.streams.serdes.SpansSerde;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TraceAggregationSupplierTest {

  @Test void should_aggregate_spans() {
    // Given
    String spansTopicName = "spans";
    String tracesTopicName = "traces";

    Duration traceInactivityGap = Duration.ofSeconds(1);
    Topology topology = new TraceAggregationSupplier(
        spansTopicName, tracesTopicName, traceInactivityGap, Duration.ofSeconds(1)).get();

    TopologyDescription description = topology.describe();
    System.out.println("Topology: \n" + description);

    assertEquals(1, description.subtopologies().size());

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    TopologyTestDriver testDriver = new TopologyTestDriver(topology, props);

    SpanSerde spanSerde = new SpanSerde();
    SpansSerde spansSerde = new SpansSerde();

    // When: two related spans coming on the same Session window
    ConsumerRecordFactory<String, Span> factory =
        new ConsumerRecordFactory<>(spansTopicName, new StringSerializer(), spanSerde.serializer());
    Span a = Span.newBuilder().traceId("a").id("a").build();
    Span b = Span.newBuilder().traceId("a").id("b").build();
    testDriver.pipeInput(factory.create(spansTopicName, a.traceId(), a, 0L));
    testDriver.pipeInput(factory.create(spansTopicName, b.traceId(), b, 0L));

    // When: and new record arrive, moving the event clock further than inactivity gap
    Span c = Span.newBuilder().traceId("c").id("c").build();
    testDriver.pipeInput(factory.create(spansTopicName, c.traceId(), c, traceInactivityGap.toMillis() + 1));

    // Then: a trace is aggregated.
    ProducerRecord<String, List<Span>> trace =
        testDriver.readOutput(tracesTopicName, new StringDeserializer(), spansSerde.deserializer());
    assertNotNull(trace);
    OutputVerifier.compareKeyValue(trace, a.traceId(), Arrays.asList(a, b));
  }
}