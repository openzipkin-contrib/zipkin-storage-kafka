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

  private Duration traceTimeout;
  private KafkaStorage storage;
  private Properties testConsumerConfig;
  private KafkaProducer<String, List<Span>> tracesProducer;
  private KafkaProducer<String, DependencyLink> dependencyProducer;

  @BeforeEach void start() {
    testConsumerConfig = new Properties();
    testConsumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    testConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
    testConsumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        ByteArrayDeserializer.class);
    testConsumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        ByteArrayDeserializer.class);

    if (!kafka.isRunning()) fail();

    traceTimeout = Duration.ofSeconds(5);
    storage = (KafkaStorage) new KafkaStorage.Builder()
        .bootstrapServers(kafka.getBootstrapServers())
        .storeDirectory("target/zipkin_" + System.currentTimeMillis())
        .traceTimeout(traceTimeout)
        .build();

    await().atMost(10, TimeUnit.SECONDS).until(() -> {
      Collection<NewTopic> newTopics = new ArrayList<>();
      newTopics.add(new NewTopic(storage.spansTopicName, 1, (short) 1));
      newTopics.add(new NewTopic(storage.traceTopicName, 1, (short) 1));
      newTopics.add(new NewTopic(storage.dependencyTopicName, 1, (short) 1));
      storage.getAdminClient().createTopics(newTopics).all().get();
      storage.checkTopics();
      return storage.topicsValidated;
    });

    await().atMost(10, TimeUnit.SECONDS).until(() -> storage.check().ok());

    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    tracesProducer = new KafkaProducer<>(producerConfig, new StringSerializer(),
        new SpansSerde().serializer());
    dependencyProducer = new KafkaProducer<>(producerConfig, new StringSerializer(),
        new DependencyLinkSerde().serializer());
  }

  @AfterEach void close() {
    dependencyProducer.close(Duration.ofSeconds(1));
    dependencyProducer = null;
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
        testConsumerConfig, storage.spansTopicName, 2, 10000);
    // Given: some time for stream processes to kick in
    Thread.sleep(traceTimeout.toMillis() * 2);
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
        testConsumerConfig, storage.spansTopicName, 1, 1000);
    IntegrationTestUtils.waitUntilMinRecordsReceived(
        testConsumerConfig, storage.traceTopicName, 1, 30000);
    // Then: and a dependency link created
    IntegrationTestUtils.waitUntilMinRecordsReceived(
        testConsumerConfig, storage.dependencyTopicName, 1, 1000);
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
    Span other = Span.newBuilder().traceId("c").id("c").name("op_c").kind(Span.Kind.SERVER)
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_c").build())
        .timestamp(TODAY * 1000 + 10).duration(8)
        .build();
    List<Span> spans = Arrays.asList(parent, child);
    // When: been published
    tracesProducer.send(new ProducerRecord<>(storage.spansTopicName, parent.traceId(), spans));
    tracesProducer.send(new ProducerRecord<>(storage.spansTopicName, other.traceId(), Collections.singletonList(other)));
    tracesProducer.flush();
    // Then: stored
    IntegrationTestUtils.waitUntilMinRecordsReceived(
        testConsumerConfig, storage.spansTopicName, 1, 10000);
    // When: and stores running
    SpanStore spanStore = storage.spanStore();
    ServiceAndSpanNames serviceAndSpanNames = storage.serviceAndSpanNames();
    // Then: services names are searchable
    await().atMost(10, TimeUnit.SECONDS)
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
            System.err.println(e.getMessage());
          } catch (Exception e) {
            e.printStackTrace();
          }
          return traces.size() == 1
              && traces.get(0).size() == 2; // Trace is found and has two spans
        });
    await().atMost(10, TimeUnit.SECONDS)
        .until(() -> {
          List<List<Span>> traces = new ArrayList<>();
          try {
            traces =
                spanStore.getTraces(QueryRequest.newBuilder()
                    .endTs(TODAY + 1)
                    .lookback(Duration.ofMinutes(2).toMillis())
                    .limit(1)
                    .build())
                    .execute();
          } catch (InvalidStateStoreException e) { // ignoring state issues
            System.err.println(e.getMessage());
          } catch (Exception e) {
            e.printStackTrace();
          }
          return traces.size() == 1
              && traces.get(0).size() == 1; // last trace is returned first
        });
    await().atMost(5, TimeUnit.SECONDS)
        .until(() -> {
          List<String> services = new ArrayList<>();
          try {
            services = serviceAndSpanNames.getServiceNames().execute();
          } catch (InvalidStateStoreException e) { // ignoring state issues
            System.err.println(e.getMessage());
          } catch (Exception e) {
            e.printStackTrace();
          }
          return services.size() == 3;
        }); // There are two service names
    await().atMost(5, TimeUnit.SECONDS)
        .until(() -> {
          List<String> spanNames = new ArrayList<>();
          try {
            spanNames = serviceAndSpanNames.getSpanNames("svc_a")
                .execute();
          } catch (InvalidStateStoreException e) { // ignoring state issues
            System.err.println(e.getMessage());
          } catch (Exception e) {
            e.printStackTrace();
          }
          return spanNames.size() == 1;
        }); // Service names have one span name
    await().atMost(5, TimeUnit.SECONDS)
        .until(() -> {
          List<String> services = new ArrayList<>();
          try {
            services = serviceAndSpanNames.getRemoteServiceNames("svc_a").execute();
          } catch (InvalidStateStoreException e) { // ignoring state issues
            System.err.println(e.getMessage());
          } catch (Exception e) {
            e.printStackTrace();
          }
          return services.size() == 1;
        }); // And one remote service name
  }

  @Test void should_find_dependencies() throws Exception {
    //Given: two related dependency links
    // When: sent first one
    dependencyProducer.send(
        new ProducerRecord<>(storage.dependencyTopicName, "svc_a:svc_b",
            DependencyLink.newBuilder()
                .parent("svc_a")
                .child("svc_b")
                .callCount(1)
                .errorCount(0)
                .build()));
    // When: and another one
    dependencyProducer.send(
        new ProducerRecord<>(storage.dependencyTopicName, "svc_a:svc_b",
            DependencyLink.newBuilder()
                .parent("svc_a")
                .child("svc_b")
                .callCount(1)
                .errorCount(0)
                .build()));
    dependencyProducer.flush();
    // Then: stored in topic
    IntegrationTestUtils.waitUntilMinRecordsReceived(
        testConsumerConfig, storage.dependencyTopicName, 2, 10000);
    // When: stores running
    SpanStore spanStore = storage.spanStore();
    // Then:
    await().atMost(10, TimeUnit.SECONDS).until(() -> {
      List<DependencyLink> links = new ArrayList<>();
      try {
        links =
            spanStore.getDependencies(System.currentTimeMillis(), Duration.ofMinutes(2).toMillis())
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
