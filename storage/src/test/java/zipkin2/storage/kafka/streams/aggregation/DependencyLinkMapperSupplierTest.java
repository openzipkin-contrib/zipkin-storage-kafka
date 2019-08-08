package zipkin2.storage.kafka.streams.aggregation;

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
import zipkin2.DependencyLink;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.storage.kafka.streams.serdes.DependencyLinkSerde;
import zipkin2.storage.kafka.streams.serdes.SpansSerde;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class DependencyLinkMapperSupplierTest {

  @Test void should_map_trace_to_dependencyLink() {
    // Given
    String tracesTopicName = "traces";
    String dependencyLinksTopicName = "dependency-links";

    Topology topology =
        new DependencyLinkMapperSupplier(tracesTopicName, dependencyLinksTopicName).get();

    TopologyDescription description = topology.describe();
    System.out.println("Topology: \n" + description);

    assertEquals(1, description.subtopologies().size());

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    TopologyTestDriver testDriver = new TopologyTestDriver(topology, props);

    SpansSerde spansSerde = new SpansSerde();
    DependencyLinkSerde dependencyLinkSerde = new DependencyLinkSerde();

    // When: a trace is passed
    ConsumerRecordFactory<String, List<Span>> factory =
        new ConsumerRecordFactory<>(tracesTopicName, new StringSerializer(),
            spansSerde.serializer());
    Span a = Span.newBuilder().traceId("a").id("a").name("op_a").kind(Span.Kind.CLIENT)
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_a").build())
        .build();
    Span b = Span.newBuilder().traceId("a").id("b").name("op_b").kind(Span.Kind.SERVER)
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_b").build())
        .build();
    List<Span> spans = Arrays.asList(a, b);
    testDriver.pipeInput(factory.create(tracesTopicName, a.traceId(), spans, 0L));

    // Then: a dependency link is created
    ProducerRecord<String, DependencyLink> linkRecord =
        testDriver.readOutput(dependencyLinksTopicName, new StringDeserializer(),
            dependencyLinkSerde.deserializer());
    assertNotNull(linkRecord);
    DependencyLink link = DependencyLink.newBuilder()
        .parent("svc_a")
        .child("svc_b")
        .errorCount(0)
        .callCount(1)
        .build();
    OutputVerifier.compareKeyValue(linkRecord, "svc_a:svc_b", link);
  }
}