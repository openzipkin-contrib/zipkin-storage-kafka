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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.CheckResult;
import zipkin2.DependencyLink;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;

import static org.awaitility.Awaitility.await;
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
        .storeDirectory("target/zipkin_" + epochMilli)
        .spansTopic(KafkaStorage.Topic.builder("zipkin").build())
        .build();
  }

  @After
  public void closeStorageReleaseLock() {
    storage.close();
    storage = null;
  }

  @Test
  public void shouldStoreSpansAndServices() throws Exception {
    Span root = Span.newBuilder()
        .traceId("a")
        .id("a")
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_a").build())
        .name("op_a")
        .timestamp(TODAY)
        .duration(10)
        .build();
    Span child = Span.newBuilder()
        .traceId("a")
        .id("b")
        .localEndpoint(Endpoint.newBuilder().serviceName("svc_a").build())
        .name("op_b")
        .timestamp(TODAY)
        .duration(2)
        .build();
    List<Span> spans0 = Arrays.asList(root, child);

    final SpanConsumer spanConsumer = storage.spanConsumer();
    spanConsumer.accept(spans0).execute();

    IntegrationTestUtils.waitUntilMinRecordsReceived(
        testConsumerConfig, storage.traceSpansTopic.name, 2, 10000);
    IntegrationTestUtils.waitUntilMinRecordsReceived(
        testConsumerConfig, storage.servicesTopic.name, 2, 10000);
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

    final SpanConsumer spanConsumer = storage.spanConsumer();
    final SpanStore spanStore = storage.spanStore();

    List<Span> spans = Arrays.asList(root, child);
    spanConsumer.accept(spans).execute();

    IntegrationTestUtils.waitUntilMinRecordsReceived(
        testConsumerConfig, storage.traceSpansTopic.name, 2, 10000);
    IntegrationTestUtils.waitUntilMinRecordsReceived(
        testConsumerConfig, storage.tracesTopic.name, 2, 60000);

    await().atMost(60, TimeUnit.SECONDS)
        .until(() -> {
          List<DependencyLink> dependencyLinks = spanStore.getDependencies(0L, 0L).execute();
          return dependencyLinks.size() == 1;
        });
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
    final SpanConsumer spanConsumer = storage.spanConsumer();
    final SpanStore spanStore = storage.spanStore();

    spanConsumer.accept(spans).execute();

    IntegrationTestUtils.waitUntilMinRecordsReceived(
        testConsumerConfig, storage.spansTopic.name, 2, 10000);
    await().atMost(30, TimeUnit.SECONDS)
        .until(() -> {
          List<List<Span>> traces =
              spanStore.getTraces(QueryRequest.newBuilder()
                  .endTs(TODAY + 1)
                  .limit(10)
                  .lookback(Duration.ofMinutes(1).toMillis())
                  .build())
                  .execute();
          return traces.size() == 1 && traces.get(0).size() == 2;
        });
  }

  @Test
  public void shouldFindTracesByTags() throws Exception {
    Map<String, String> annotationQuery =
        new HashMap<String, String>() {
          {
            put("key_tag_a", "value_tag_a");
          }
        };

    Span span1 =
        Span.newBuilder()
            .traceId("a")
            .id("a")
            .putTag("key_tag_a", "value_tag_a")
            .localEndpoint(Endpoint.newBuilder().serviceName("svc_a").build())
            .name("op_a")
            .kind(Span.Kind.CLIENT)
            .timestamp(Long.valueOf(TODAY + "000"))
            .duration(10)
            .build();

    Span span2 =
        Span.newBuilder()
            .traceId("b")
            .id("b")
            .localEndpoint(Endpoint.newBuilder().serviceName("svc_b").build())
            .putTag("key_tag_c", "value_tag_d")
            .addAnnotation(Long.valueOf(TODAY + "000"), "annotation_b")
            .name("op_b")
            .kind(Span.Kind.CLIENT)
            .timestamp(Long.valueOf(TODAY + "000"))
            .duration(10)
            .build();

    final SpanConsumer spanConsumer = storage.spanConsumer();
    final SpanStore spanStore = storage.spanStore();

    List<Span> spans = Arrays.asList(span1, span2);
    spanConsumer.accept(spans).execute();

    IntegrationTestUtils.waitUntilMinRecordsReceived(
        testConsumerConfig, storage.spansTopic.name, 2, 10000);

    // query by annotation {"key_tag_a":"value_tag_a"} = 1 trace
    await()
        .atMost(30, TimeUnit.SECONDS)
        .until(() -> {
          List<List<Span>> traces =
              spanStore.getTraces(QueryRequest.newBuilder()
                  .annotationQuery(annotationQuery)
                  .endTs(TODAY + 1)
                  .limit(10)
                  .lookback(Duration.ofMinutes(1).toMillis())
                  .build())
                  .execute();
          return traces.size() == 1;
        });

    // query by annotation {"key_tag_non_exist_a":"value_tag_non_exist_a"} = 0 trace
    await()
        .pollDelay(5, TimeUnit.SECONDS)
        .until(() -> {
          List<List<Span>> traces =
              spanStore.getTraces(QueryRequest.newBuilder()
                  .annotationQuery(
                      new HashMap<String, String>() {{
                        put("key_tag_non_exist_a", "value_tag_non_exist_a");
                      }})
                  .endTs(TODAY + 1)
                  .limit(10)
                  .lookback(Duration.ofMinutes(1).toMillis())
                  .build())
                  .execute();
          return traces.size() == 0;
        });
  }

  @Test
  public void shouldFindTracesByAnnotations() throws Exception {
    Span span1 =
        Span.newBuilder()
            .traceId("a")
            .id("a")
            .putTag("key_tag_a", "value_tag_a")
            .addAnnotation(TODAY, "log value")
            .localEndpoint(Endpoint.newBuilder().serviceName("svc_a").build())
            .name("op_a")
            .kind(Span.Kind.CLIENT)
            .timestamp(Long.valueOf(TODAY + "000"))
            .duration(10)
            .build();

    Span span2 =
        Span.newBuilder()
            .traceId("b")
            .id("b")
            .localEndpoint(Endpoint.newBuilder().serviceName("svc_b").build())
            .putTag("key_tag_c", "value_tag_d")
            .addAnnotation(Long.valueOf(TODAY + "000"), "annotation_b")
            .name("op_b")
            .kind(Span.Kind.CLIENT)
            .timestamp(Long.valueOf(TODAY + "000"))
            .duration(10)
            .build();

    final SpanConsumer spanConsumer = storage.spanConsumer();
    final SpanStore spanStore = storage.spanStore();

    List<Span> spans = Arrays.asList(span1, span2);
    spanConsumer.accept(spans).execute();

    IntegrationTestUtils.waitUntilMinRecordsReceived(
        testConsumerConfig, storage.spansTopic.name, 2, 10000);

    // query by annotation {"key_tag_a":"value_tag_a"} = 1 trace
    await()
        .atMost(30, TimeUnit.SECONDS)
        .until(() -> {
          List<List<Span>> traces =
              spanStore.getTraces(QueryRequest.newBuilder()
                  .parseAnnotationQuery("log*")
                  .endTs(TODAY + 1)
                  .limit(10)
                  .lookback(Duration.ofMinutes(1).toMillis())
                  .build())
                  .execute();
          return traces.size() == 1;
        });
  }

  @Test
  public void shouldFindTracesBySpanName() throws Exception {
    Span span1 =
        Span.newBuilder()
            .traceId("a")
            .id("a")
            .localEndpoint(Endpoint.newBuilder().serviceName("svc_a").build())
            .name("op_a")
            .kind(Span.Kind.CLIENT)
            .timestamp(Long.valueOf(TODAY + "000"))
            .duration(10)
            .build();

    Span span2 =
        Span.newBuilder()
            .traceId("b")
            .id("b")
            .localEndpoint(Endpoint.newBuilder().serviceName("svc_b").build())
            .name("op_b")
            .kind(Span.Kind.CLIENT)
            .timestamp(Long.valueOf(TODAY + "000"))
            .duration(10)
            .build();

    final SpanConsumer spanConsumer = storage.spanConsumer();
    final SpanStore spanStore = storage.spanStore();

    List<Span> spans = Arrays.asList(span1, span2);
    spanConsumer.accept(spans).execute();

    IntegrationTestUtils.waitUntilMinRecordsReceived(
        testConsumerConfig, storage.spansTopic.name, 2, 10000);

    // query by span name `op_a` = 1 trace
    await()
        .atMost(5, TimeUnit.SECONDS)
        .until(() -> {
          List<List<Span>> traces =
              spanStore.getTraces(
                  QueryRequest.newBuilder()
                      .spanName("op_a")
                      .endTs(TODAY + 1)
                      .limit(10)
                      .lookback(Duration.ofMinutes(1).toMillis())
                      .build())
                  .execute();
          return traces.size() == 1;
        });

    // query by span name `op_b` = 1 trace
    await()
        .atMost(5, TimeUnit.SECONDS)
        .until(() -> {
          List<List<Span>> traces =
              spanStore.getTraces(
                  QueryRequest.newBuilder()
                      .spanName("op_b")
                      .endTs(TODAY + 1)
                      .limit(10)
                      .lookback(Duration.ofMinutes(1).toMillis())
                      .build())
                  .execute();
          return traces.size() == 1;
        });

    // query by span name `non_existing_span_name` = 0 trace
    await()
        .pollDelay(5, TimeUnit.SECONDS)
        .until(() -> {
          List<List<Span>> traces =
              spanStore.getTraces(
                  QueryRequest.newBuilder()
                      .spanName("non_existing_span_name")
                      .endTs(TODAY + 1)
                      .limit(10)
                      .lookback(Duration.ofMinutes(1).toMillis())
                      .build())
                  .execute();
          return traces.size() == 0;
        });
  }

  @Test
  public void shouldFindTracesByServiceName() throws Exception {
    Span span1 =
        Span.newBuilder()
            .traceId("a")
            .id("a")
            .localEndpoint(Endpoint.newBuilder().serviceName("svc_a").build())
            .name("op_a")
            .kind(Span.Kind.CLIENT)
            .timestamp(Long.valueOf(TODAY + "000"))
            .duration(10)
            .build();

    Span span2 =
        Span.newBuilder()
            .traceId("b")
            .id("b")
            .localEndpoint(Endpoint.newBuilder().serviceName("svc_a").build())
            .name("op_b")
            .kind(Span.Kind.CLIENT)
            .timestamp(Long.valueOf(TODAY + "000"))
            .duration(10)
            .build();

    final SpanConsumer spanConsumer = storage.spanConsumer();
    final SpanStore spanStore = storage.spanStore();

    List<Span> spans = Arrays.asList(span1, span2);
    spanConsumer.accept(spans).execute();

    IntegrationTestUtils.waitUntilMinRecordsReceived(
        testConsumerConfig, storage.spansTopic.name, 2, 10000);

    // query by service name `srv_a` = 2 trace
    await()
        .atMost(5, TimeUnit.SECONDS)
        .until(() -> {
          List<List<Span>> traces =
              spanStore.getTraces(
                  QueryRequest.newBuilder()
                      .serviceName("svc_a")
                      .endTs(TODAY + 1)
                      .limit(10)
                      .lookback(Duration.ofMinutes(1).toMillis())
                      .build())
                  .execute();
          return traces.size() == 2;
        });

    // query by service name `non_existing_span_name` = 0 trace
    await()
        .pollDelay(5, TimeUnit.SECONDS)
        .until(() -> {
          List<List<Span>> traces =
              spanStore.getTraces(
                  QueryRequest.newBuilder()
                      .serviceName("non_existing_span_name")
                      .endTs(TODAY + 1)
                      .limit(10)
                      .lookback(Duration.ofMinutes(1).toMillis())
                      .build())
                  .execute();
          return traces.size() == 0;
        });
  }

  @Test
  public void traceQueryEnqueue() {
    final SpanStore spanStore = storage.spanStore();
    Call<List<List<Span>>> callTraces =
        spanStore.getTraces(
            QueryRequest.newBuilder()
                .serviceName("non_existing_span_name")
                .endTs(TODAY + 1)
                .limit(10)
                .lookback(Duration.ofMinutes(1).toMillis())
                .build());

    Callback<List<List<Span>>> callback = new Callback<List<List<Span>>>() {
      @Override
      public void onSuccess(List<List<Span>> value) {
        System.out.println("Here: " + value);
      }

      @Override
      public void onError(Throwable t) {
        t.printStackTrace();
      }
    };

    try {
      callTraces.enqueue(callback);
    } catch (Exception e) {
      fail();
    }

    try {
      callTraces.enqueue(callback);
      fail();
    } catch (Exception ignored) {
    }
  }

  @Test
  public void checkShouldErrorWhenKafkaNotAvailable() {
    CheckResult checked = storage.check();
    assertEquals(CheckResult.OK, checked);

    kafka.stop();
    await().atMost(5, TimeUnit.SECONDS)
        .until(() -> {
          CheckResult check = storage.check();
          return check != CheckResult.OK;
        });
  }
}
