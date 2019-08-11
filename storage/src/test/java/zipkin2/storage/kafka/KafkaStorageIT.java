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
import java.util.ArrayList;
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
import org.apache.kafka.streams.errors.InvalidStateStoreException;
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

  private Duration traceInactivityGap;
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
    traceInactivityGap = Duration.ofSeconds(5);
    storage = (KafkaStorage) new KafkaStorage.Builder().ensureTopics(true)
        .bootstrapServers(kafka.getBootstrapServers())
        .storeDirectory("target/zipkin_" + epochMilli)
        .traceInactivityGap(traceInactivityGap)
        .build();

    storage.ensureTopics();

    await().atMost(10, TimeUnit.SECONDS).until(() -> storage.check().ok());

    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    tracesProducer = new KafkaProducer<>(producerConfig, new StringSerializer(),
        new SpansSerde().serializer());
    linkProducer = new KafkaProducer<>(producerConfig, new StringSerializer(),
        new DependencyLinkSerde().serializer());
  }

  @AfterEach void closeStorageReleaseLock() {
    linkProducer.close(Duration.ofSeconds(1));
    linkProducer = null;
    tracesProducer.close(Duration.ofSeconds(1));
    tracesProducer = null;
    storage.close();
    storage = null;
  }

  @Test void should_aggregate() throws Exception {
    // Given: a set of incoming spans
    Span parent = Span.newBuilder().traceId("a").id("a").name("op_a").kind(Span.Kind.CLIENT)
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_a").build())
        .timestamp(System.currentTimeMillis() * 1000).duration(10)
        .build();
    Span child = Span.newBuilder().traceId("a").id("b").name("op_b").kind(Span.Kind.SERVER)
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_b").build())
        .timestamp(System.currentTimeMillis() * 1000).duration(2)
        .build();
    final SpanConsumer spanConsumer = storage.spanConsumer();
    // When: are consumed by storage
    spanConsumer.accept(Arrays.asList(parent, child)).execute();
    storage.getProducer().flush();
    // Then: they are partitioned
    IntegrationTestUtils.waitUntilMinRecordsReceived(
        testConsumerConfig, storage.spansTopic.name, 2, 10000);
    // Given: some time for stream processes to kick in
    Thread.sleep(traceInactivityGap.toMillis() * 2);
    // Given: another span to move 'event time' forward
    Span another = Span.newBuilder().traceId("c").id("d").name("op_a").kind(Span.Kind.SERVER)
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_b").build())
        .timestamp(System.currentTimeMillis() * 1000).duration(2)
        .build();
    // When: published
    spanConsumer.accept(Collections.singletonList(another)).execute();
    storage.getProducer().flush();
    // Then: a trace is published
    IntegrationTestUtils.waitUntilMinRecordsReceived(
        testConsumerConfig, storage.spansTopic.name, 1, 1000);
    IntegrationTestUtils.waitUntilMinRecordsReceived(
        testConsumerConfig, storage.tracesTopic.name, 1, 30000);
    // Then: and a dependency link created
    IntegrationTestUtils.waitUntilMinRecordsReceived(
        testConsumerConfig, storage.dependencyLinksTopic.name, 1, 1000);
  }

  @Test void should_return_traces_query() throws Exception {
    // Given: a trace prepared to be published
    Span parent = Span.newBuilder().traceId("a").id("a").name("op_a").kind(Span.Kind.CLIENT)
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_a").build())
        .remoteEndpoint(Endpoint.newBuilder().serviceName("svc_b").build())
        .timestamp(TODAY * 1000).duration(10)
        .build();
    Span child = Span.newBuilder().traceId("a").id("b").name("op_b").kind(Span.Kind.SERVER)
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_b").build())
        .timestamp(TODAY * 1000).duration(2)
        .build();
    List<Span> spans = Arrays.asList(parent, child);
    // When: been published
    tracesProducer.send(new ProducerRecord<>(storage.tracesTopic.name, parent.traceId(), spans));
    tracesProducer.flush();
    // Then: stored
    IntegrationTestUtils.waitUntilMinRecordsReceived(
        testConsumerConfig, storage.tracesTopic.name, 1, 10000);
    // When: and stores running
    SpanStore spanStore = storage.spanStore();
    ServiceAndSpanNames serviceAndSpanNames = storage.serviceAndSpanNames();
    // Then: services names are searchable
    await().atMost(30, TimeUnit.SECONDS)
        .until(() -> {
          List<List<Span>> traces = new ArrayList<>();
          try {
            traces =
                spanStore.getTraces(QueryRequest.newBuilder()
                    .endTs(TODAY + 1)
                    .lookback(Duration.ofMinutes(1).toMillis())
                    .serviceName("svc_a")
                    .limit(10)
                    .build())
                    .execute();
          } catch (InvalidStateStoreException e) { // ignoring state issues
          }
          return traces.size() == 1
              && traces.get(0).size() == 2; // Trace is found and has two spans
        });
    await().atMost(5, TimeUnit.SECONDS)
        .until(() -> {
          List<String> services = new ArrayList<>();
          try {
            services = serviceAndSpanNames.getServiceNames().execute();
          } catch (InvalidStateStoreException e) { // ignoring state issues
          }
          return services.size() == 2;
        }); // There are two service names
    await().atMost(5, TimeUnit.SECONDS)
        .until(() -> {
          List<String> spanNames = new ArrayList<>();
          try {
            spanNames = serviceAndSpanNames.getSpanNames("svc_a")
                .execute();
          } catch (InvalidStateStoreException e) { // ignoring state issues
          }
          return spanNames.size() == 1;
        }); // Service names have one span name
    await().atMost(5, TimeUnit.SECONDS)
        .until(() -> {
          List<String> services = new ArrayList<>();
          try {
            services = serviceAndSpanNames.getRemoteServiceNames("svc_a").execute();
          } catch (InvalidStateStoreException e) { // ignoring state issues
          }
          return services.size() == 1;
        }); // And one remote service name
  }

  @Test void should_find_dependencies() throws Exception {
    //Given: two related dependency links
    DependencyLink link1 = DependencyLink.newBuilder()
        .parent("svc_a")
        .child("svc_b")
        .callCount(1)
        .errorCount(0)
        .build();
    // When: sent first one
    linkProducer.send(
        new ProducerRecord<>(storage.dependencyLinksTopic.name, "svc_a:svc_b", link1));
    DependencyLink link2 = DependencyLink.newBuilder()
        .parent("svc_a")
        .child("svc_b")
        .callCount(1)
        .errorCount(0)
        .build();
    // When: and another one
    linkProducer.send(
        new ProducerRecord<>(storage.dependencyLinksTopic.name, "svc_a:svc_b", link2));
    linkProducer.flush();
    // Then: stored in topic
    IntegrationTestUtils.waitUntilMinRecordsReceived(
        testConsumerConfig, storage.dependencyLinksTopic.name, 2, 10000);
    // When: stores running
    SpanStore spanStore = storage.spanStore();
    // Then:
    await().atMost(10, TimeUnit.SECONDS).until(() -> {
      List<DependencyLink> links = new ArrayList<>();
      try {
        links =
            spanStore.getDependencies(System.currentTimeMillis(), Duration.ofMinutes(1).toMillis())
                .execute();
      } catch (InvalidStateStoreException e) { // ignoring state issues
      }
      return links.size() == 1
          && links.get(0).callCount() == 2; // link stored and call count aggregated.
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
