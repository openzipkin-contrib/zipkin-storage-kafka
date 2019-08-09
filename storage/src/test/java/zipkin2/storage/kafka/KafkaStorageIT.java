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
package zipkin2.storage.kafka;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import zipkin2.CheckResult;
import zipkin2.DependencyLink;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.ServiceAndSpanNames;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.kafka.streams.serdes.DependencyLinkSerde;
import zipkin2.storage.kafka.streams.serdes.SpansSerde;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Testcontainers
class KafkaStorageIT {
  private static final long TODAY = System.currentTimeMillis();

  @Container private KafkaContainer kafka = new KafkaContainer("5.3.0");

  private KafkaStorage storage;
  private Properties testConsumerConfig;
  private KafkaProducer<String, List<Span>> tracesProducer;
  private KafkaProducer<String, DependencyLink> linkProducer;

  @BeforeEach void start() {
    testConsumerConfig = new Properties();
    testConsumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    testConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
    testConsumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        ByteArrayDeserializer.class);
    testConsumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        ByteArrayDeserializer.class);

    if (!kafka.isRunning()) fail();

    long epochMilli = Instant.now().toEpochMilli();
    storage = (KafkaStorage) new KafkaStorage.Builder().ensureTopics(true)
        .bootstrapServers(kafka.getBootstrapServers())
        .storeDirectory("target/zipkin_" + epochMilli)
        .traceInactivityGap(Duration.ofSeconds(3))
        .traceAggregationSuppressUntil(Duration.ofSeconds(3))
        .build();

    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    tracesProducer = new KafkaProducer<>(producerConfig, new StringSerializer(),
        new SpansSerde().serializer());
    linkProducer = new KafkaProducer<>(producerConfig, new StringSerializer(),
        new DependencyLinkSerde().serializer());
  }

  @AfterEach void closeStorageReleaseLock() {
    storage.close();
    storage = null;
    tracesProducer.close();
    tracesProducer = null;
  }

  @Test void should_persist_spans() throws Exception {
    Span parent = Span.newBuilder()
        .traceId("a")
        .id("a")
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_a").build())
        .name("op_a")
        .kind(Span.Kind.CLIENT)
        .timestamp(TODAY*1000)
        .duration(10)
        .build();
    Span child = Span.newBuilder()
        .traceId("a")
        .id("b")
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_b").build())
        .name("op_b")
        .kind(Span.Kind.SERVER)
        .timestamp(TODAY*1000)
        .duration(2)
        .build();

    final SpanConsumer spanConsumer = storage.spanConsumer();

    spanConsumer.accept(Arrays.asList(parent, child)).execute();

    // Store spans
    IntegrationTestUtils.waitUntilMinRecordsReceived(
        testConsumerConfig, storage.spansTopic.name, 2, 10000);

    Thread.sleep(10_000);

    Span another = Span.newBuilder()
        .traceId("c")
        .id("d")
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_b").build())
        .name("op_a")
        .kind(Span.Kind.SERVER)
        .timestamp(TODAY*1000)
        .duration(2)
        .build();

    spanConsumer.accept(Collections.singletonList(another)).execute();

    // Aggregate traces
    IntegrationTestUtils.waitUntilMinRecordsReceived(
        testConsumerConfig, storage.tracesTopic.name, 1, 30000);

    // Map Dependency Links
    IntegrationTestUtils.waitUntilMinRecordsReceived(
        testConsumerConfig, storage.dependencyLinksTopic.name, 1, 10000);
  }

  @Test void should_return_traces_query() throws Exception {
    Span parent = Span.newBuilder()
        .traceId("a")
        .id("a")
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_a").build())
        .remoteEndpoint(Endpoint.newBuilder().serviceName("svc_b").build())
        .name("op_a")
        .kind(Span.Kind.CLIENT)
        .timestamp(TODAY*1000)
        .duration(10)
        .build();
    Span child = Span.newBuilder()
        .traceId("a")
        .id("b")
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_b").build())
        .name("op_b")
        .kind(Span.Kind.SERVER)
        .timestamp(TODAY*1000)
        .duration(2)
        .build();

    List<Span> spans = Arrays.asList(parent, child);
    tracesProducer.send(new ProducerRecord<>(storage.tracesTopic.name, parent.traceId(), spans));
    tracesProducer.flush();

    IntegrationTestUtils.waitUntilMinRecordsReceived(
        testConsumerConfig, storage.tracesTopic.name, 1, 10000);

    SpanStore spanStore = storage.spanStore();
    ServiceAndSpanNames serviceAndSpanNames = storage.serviceAndSpanNames();

    await().atMost(30, TimeUnit.SECONDS)
        .until(() -> {
          List<List<Span>> traces =
              spanStore.getTraces(QueryRequest.newBuilder()
                  .endTs(TODAY + 1)
                  .lookback(Duration.ofMinutes(1).toMillis())
                  .serviceName("svc_a")
                  .limit(10)
                  .build())
                  .execute();
          return traces.size() == 1 && traces.get(0).size() == 2;
        });
    await().atMost(5, TimeUnit.SECONDS)
        .until(() -> serviceAndSpanNames.getServiceNames().execute().size() == 2);
    await().atMost(5, TimeUnit.SECONDS)
        .until(() -> serviceAndSpanNames.getSpanNames("svc_a").execute().size() == 1);
    await().atMost(5, TimeUnit.SECONDS)
        .until(() -> serviceAndSpanNames.getRemoteServiceNames("svc_a").execute().size() == 1);
  }

  @Test void should_find_dependencies() throws Exception {
    DependencyLink link1 = DependencyLink.newBuilder()
        .parent("svc_a")
        .child("svc_b")
        .callCount(1)
        .errorCount(0)
        .build();
    linkProducer.send(
        new ProducerRecord<>(storage.dependencyLinksTopic.name, "svc_a:svc_b", link1));
    DependencyLink link2 = DependencyLink.newBuilder()
        .parent("svc_a")
        .child("svc_b")
        .callCount(1)
        .errorCount(0)
        .build();
    linkProducer.send(
        new ProducerRecord<>(storage.dependencyLinksTopic.name, "svc_a:svc_b", link2));
    linkProducer.flush();

    IntegrationTestUtils.waitUntilMinRecordsReceived(
        testConsumerConfig, storage.dependencyLinksTopic.name, 2, 10000);

    SpanStore spanStore = storage.spanStore();

    await().atMost(10, TimeUnit.SECONDS).until(() -> {
      List<DependencyLink> execute =
          spanStore.getDependencies(System.currentTimeMillis(), Duration.ofMinutes(1).toMillis())
              .execute();
      return execute.size() == 1 && execute.get(0).callCount() == 2;
    });
  }

  @Test void shouldFailWhenKafkaNotAvailable() {
    CheckResult checked = storage.check();
    assertEquals(CheckResult.OK, checked);

    kafka.stop();
    await().atMost(5, TimeUnit.SECONDS)
        .until(() -> {
          CheckResult check = storage.check();
          return check != CheckResult.OK;
        });
    storage.close();
  }
}
