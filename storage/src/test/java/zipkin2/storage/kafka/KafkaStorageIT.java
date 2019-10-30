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
package zipkin2.storage.kafka;

import com.linecorp.armeria.server.Server;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.NewTopic;
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
import zipkin2.storage.kafka.streams.serdes.DependencyLinkSerde;
import zipkin2.storage.kafka.streams.serdes.SpansSerde;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
class KafkaStorageIT {
  static final long TODAY = System.currentTimeMillis();

  // TODO: does this need to be confluent container? #45
  @Container KafkaContainer kafka = new KafkaContainer("5.3.0");

  Duration traceTimeout;
  KafkaStorage storage;
  Server server;
  Properties consumerConfig;
  KafkaProducer<String, List<Span>> tracesProducer;
  KafkaProducer<String, DependencyLink> dependencyProducer;
  SpansSerde spansSerde = new SpansSerde();
  DependencyLinkSerde dependencyLinkSerde = new DependencyLinkSerde();

  @BeforeEach void start() throws Exception {
    consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

    assertThat(kafka.isRunning()).isTrue();

    traceTimeout = Duration.ofSeconds(5);
    int httpPort = randomPort();
    storage = (KafkaStorage) KafkaStorage.newBuilder()
      .bootstrapServers(kafka.getBootstrapServers())
      .storageDir("target/zipkin_" + System.currentTimeMillis())
      .traceTimeout(traceTimeout)
      .serverPort(httpPort)
      .build();
    server = Server.builder()
        .http(httpPort)
        .annotatedService(KafkaStorage.HTTP_PATH_PREFIX, storage.httpService())
        .build();
    server.start();

    Collection<NewTopic> newTopics = new ArrayList<>();
    newTopics.add(new NewTopic(storage.aggregationSpansTopic, 1, (short) 1));
    newTopics.add(new NewTopic(storage.aggregationTraceTopic, 1, (short) 1));
    newTopics.add(new NewTopic(storage.aggregationDependencyTopic, 1, (short) 1));
    storage.getAdminClient().createTopics(newTopics).all().get();

    await().atMost(10, TimeUnit.SECONDS).until(() -> storage.check().ok());
    storage.checkResources();
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    tracesProducer = new KafkaProducer<>(producerConfig, new StringSerializer(),
      spansSerde.serializer());
    dependencyProducer = new KafkaProducer<>(producerConfig, new StringSerializer(),
      dependencyLinkSerde.serializer());
  }

  @AfterEach void close() {
    dependencyProducer.close(Duration.ofSeconds(1));
    dependencyProducer = null;
    tracesProducer.close(Duration.ofSeconds(1));
    tracesProducer = null;
    storage.close();
    storage = null;
    server.close();
    spansSerde.close();
    dependencyLinkSerde.close();
  }

  @Test void should_aggregate() throws Exception {
    // Given: a set of incoming spans
    Span parent = Span.newBuilder().traceId("a").id("a").name("op_a").kind(Span.Kind.CLIENT)
      .localEndpoint(Endpoint.newBuilder().serviceName("svc_a").build())
      .timestamp(MILLISECONDS.toMicros(System.currentTimeMillis())).duration(10)
      .build();
    Span child = Span.newBuilder().traceId("a").id("b").name("op_b").kind(Span.Kind.SERVER)
      .localEndpoint(Endpoint.newBuilder().serviceName("svc_b").build())
      .timestamp(MILLISECONDS.toMicros(System.currentTimeMillis())).duration(2)
      .build();
    final SpanConsumer spanConsumer = storage.spanConsumer();
    // When: are consumed by storage
    spanConsumer.accept(Arrays.asList(parent, child)).execute();
    storage.getProducer().flush();
    // Then: they are partitioned
    IntegrationTestUtils.waitUntilMinRecordsReceived(
      consumerConfig, storage.partitionedSpansTopic, 1, 10000);
    // Given: some time for stream processes to kick in
    Thread.sleep(traceTimeout.toMillis() * 2);
    // Given: another span to move 'event time' forward
    Span another = Span.newBuilder().traceId("c").id("d").name("op_a").kind(Span.Kind.SERVER)
      .localEndpoint(Endpoint.newBuilder().serviceName("svc_b").build())
      .timestamp(MILLISECONDS.toMicros(System.currentTimeMillis())).duration(2)
      .build();
    // When: published
    spanConsumer.accept(Collections.singletonList(another)).execute();
    storage.getProducer().flush();
    // Then: a trace is published
    IntegrationTestUtils.waitUntilMinRecordsReceived(
      consumerConfig, storage.aggregationSpansTopic, 1, 1000);
    IntegrationTestUtils.waitUntilMinRecordsReceived(
      consumerConfig, storage.aggregationTraceTopic, 1, 30000);
    // Then: and a dependency link created
    IntegrationTestUtils.waitUntilMinRecordsReceived(
      consumerConfig, storage.aggregationDependencyTopic, 1, 1000);
  }

  @Test void should_return_traces_query() throws Exception {
    // Given: a trace prepared to be published
    Span parent = Span.newBuilder().traceId("a").id("a").name("op_a").kind(Span.Kind.CLIENT)
      .localEndpoint(Endpoint.newBuilder().serviceName("svc_a").build())
      .remoteEndpoint(Endpoint.newBuilder().serviceName("svc_b").build())
      .timestamp(MILLISECONDS.toMicros(TODAY)).duration(10)
      .build();
    Span child = Span.newBuilder().traceId("a").id("b").name("op_b").kind(Span.Kind.SERVER)
      .localEndpoint(Endpoint.newBuilder().serviceName("svc_b").build())
      .timestamp(MILLISECONDS.toMicros(TODAY)).duration(2)
      .build();
    Span other = Span.newBuilder().traceId("c").id("c").name("op_c").kind(Span.Kind.SERVER)
      .localEndpoint(Endpoint.newBuilder().serviceName("svc_c").build())
      .timestamp(MILLISECONDS.toMicros(TODAY) + 10).duration(8)
      .build();
    List<Span> spans = Arrays.asList(parent, child);
    // When: and stores running
    ServiceAndSpanNames serviceAndSpanNames = storage.serviceAndSpanNames();
    // When: been published
    tracesProducer.send(new ProducerRecord<>(storage.storageSpansTopic, parent.traceId(), spans));
    tracesProducer.send(new ProducerRecord<>(storage.storageSpansTopic, other.traceId(),
      Collections.singletonList(other)));
    tracesProducer.flush();
    // Then: stored
    IntegrationTestUtils.waitUntilMinRecordsReceived(
      consumerConfig, storage.storageSpansTopic, 2, 10000);
    // Then: services names are searchable
    await().atMost(100, TimeUnit.SECONDS).until(() -> {
      List<List<Span>> traces = storage.spanStore().getTraces(QueryRequest.newBuilder()
        .endTs(TODAY + 1)
        .lookback(Duration.ofSeconds(30).toMillis())
        .serviceName("svc_a")
        .limit(10)
        .build())
        .execute();
      return (1 == traces.size()) &&
        (traces.get(0).size() == 2); // Trace is found and has two spans
    });
    List<List<Span>> filteredTraces =
      storage.spanStore().getTraces(QueryRequest.newBuilder()
        .endTs(TODAY + 1)
        .lookback(Duration.ofMinutes(1).toMillis())
        .limit(1)
        .build())
        .execute();
    assertThat(filteredTraces).hasSize(1);
    assertThat(filteredTraces.get(0)).hasSize(1); // last trace is returned first
    List<String> services = serviceAndSpanNames.getServiceNames().execute();
    assertThat(services).hasSize(3);
    List<String> spanNames = serviceAndSpanNames.getSpanNames("svc_a").execute();
    assertThat(spanNames).hasSize(1); // Service names have one span name
    List<String> remoteServices = serviceAndSpanNames.getRemoteServiceNames("svc_a").execute();
    assertThat(remoteServices).hasSize(1); // And one remote service name
    List<List<Span>> manyTraces =
      storage.traces().getTraces(Arrays.asList(parent.traceId(), other.traceId())).execute();
    assertThat(manyTraces).hasSize(2);
  }

  @Test void should_find_dependencies() throws Exception {
    //Given: two related dependency links
    // When: sent first one
    dependencyProducer.send(
      new ProducerRecord<>(storage.storageDependencyTopic, "svc_a:svc_b",
        DependencyLink.newBuilder()
          .parent("svc_a")
          .child("svc_b")
          .callCount(1)
          .errorCount(0)
          .build()));
    // When: and another one
    dependencyProducer.send(
      new ProducerRecord<>(storage.storageDependencyTopic, "svc_a:svc_b",
        DependencyLink.newBuilder()
          .parent("svc_a")
          .child("svc_b")
          .callCount(1)
          .errorCount(0)
          .build()));
    dependencyProducer.flush();
    // Then: stored in topic
    IntegrationTestUtils.waitUntilMinRecordsReceived(
      consumerConfig, storage.storageDependencyTopic, 2, 10000);
    // Then:
    await().atMost(10, TimeUnit.SECONDS).until(() -> {
      List<DependencyLink> links = new ArrayList<>();
      try {
        links = storage.spanStore()
          .getDependencies(System.currentTimeMillis(), Duration.ofMinutes(2).toMillis())
          .execute();
      } catch (InvalidStateStoreException e) { // ignoring state issues
        System.err.println(e.getMessage());
      } catch (Exception e) {
        e.printStackTrace();
      }
      return links.size() == 1
        && links.get(0).callCount() == 2; // link stored and call count aggregated.
    });
  }

  @Test void shouldFailWhenKafkaNotAvailable() {
    CheckResult checked = storage.check();
    assertThat(checked.ok()).isTrue();

    kafka.stop();
    await().atMost(5, TimeUnit.SECONDS)
      .until(() -> {
        CheckResult check = storage.check();
        return check != CheckResult.OK;
      });
    storage.close();
  }

  static int randomPort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
