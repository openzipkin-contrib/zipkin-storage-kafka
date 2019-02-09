/*
 * Copyright 2019 [name of copyright owner]
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
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static zipkin2.TestObjects.TODAY;

public class KafkaStorageIT {
  @Rule
  public KafkaContainer kafka = new KafkaContainer("5.1.0");

  private KafkaStorage storage;
  private Properties testConsumerConfig;

  @Before
  public void start() {
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
        .processStreamStoreDirectory("target/kafka-streams/process/" + epochMilli)
        .indexStreamStoreDirectory("target/kafka-streams/index/" + epochMilli)
        .indexStorageDirectory("target/index/" + epochMilli)
        .spansTopic(KafkaStorage.Topic.builder("topic").build())
        .build();
  }

  @After
  public void closeStorageReleaseLock() {
    storage.close();
    storage = null;
  }

  @Test
  public void shouldStoreSpansAndServices() throws Exception {
    Span root0 = Span.newBuilder()
        .traceId("a")
        .id("a")
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_a").build())
        .name("op_a")
        .timestamp(TODAY)
        .duration(10)
        .build();
    Span child0 = Span.newBuilder()
        .traceId("a")
        .id("b")
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_a").build())
        .name("op_b")
        .timestamp(TODAY)
        .duration(2)
        .build();
    List<Span> spans0 = Arrays.asList(root0, child0);

    storage.spanConsumer().accept(spans0).execute();

    IntegrationTestUtils.waitUntilMinRecordsReceived(testConsumerConfig, storage.spansTopic.name, 2,
        10000);
    IntegrationTestUtils.waitUntilMinRecordsReceived(testConsumerConfig,
        storage.servicesTopic.name,
        2, 10000);
  }

  @Test
  public void shouldCreateDependencyGraph() throws Exception {
    Span root = Span.newBuilder()
        .traceId("a")
        .id("a")
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_a").build())
        .remoteEndpoint(Endpoint.newBuilder().serviceName("svc_b").build())
        .name("op_a")
        .kind(Span.Kind.CLIENT)
        .timestamp(TODAY)
        .duration(10)
        .build();
    Span child = Span.newBuilder()
        .traceId("a")
        .id("b")
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_b").build())
        .name("op_b")
        .kind(Span.Kind.SERVER)
        .timestamp(TODAY)
        .duration(2)
        .build();
    List<Span> spans = Arrays.asList(root, child);
    storage.spanConsumer().accept(spans).execute();

    IntegrationTestUtils.waitUntilMinRecordsReceived(testConsumerConfig,
        storage.dependenciesTopic.name, 1, 10000);
  }

  @Test
  public void shouldFindTraces() throws Exception {
    Span root = Span.newBuilder()
        .traceId("a")
        .id("a")
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_a").build())
        .remoteEndpoint(Endpoint.newBuilder().serviceName("svc_b").build())
        .name("op_a")
        .kind(Span.Kind.CLIENT)
        .timestamp(Long.valueOf(TODAY + "000"))
        .duration(10)
        .build();
    Span child = Span.newBuilder()
        .traceId("a")
        .id("b")
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_b").build())
        .name("op_b")
        .kind(Span.Kind.SERVER)
        .timestamp(Long.valueOf(TODAY + "000"))
        .timestamp(TODAY)
        .duration(2)
        .build();
    List<Span> spans = Arrays.asList(root, child);
    storage.spanConsumer().accept(spans).execute();

    IntegrationTestUtils.waitUntilMinRecordsReceived(testConsumerConfig, storage.spansTopic.name, 2,
        10000);
    Thread.sleep(1000);
    List<List<Span>> traces =
        storage.spanStore()
            .getTraces(QueryRequest.newBuilder()
                .endTs(TODAY + 1)
                .limit(10)
                .lookback(Duration.ofMinutes(1).toMillis())
                .build())
            .execute();
    assertEquals(1, traces.size());
    assertEquals(2, traces.get(0).size());
  }
}