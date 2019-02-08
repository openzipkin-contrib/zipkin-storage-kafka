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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import zipkin2.Callback;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.storage.StorageComponent;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;
import static zipkin2.TestObjects.TODAY;

public class KafkaStorageIT {
    @Rule
    public KafkaContainer kafka = new KafkaContainer("5.1.0");

    @Test
    public void should_consume_spans() throws InterruptedException {
        StorageComponent storage = new KafkaStorage.Builder().bootstrapServers(kafka.getBootstrapServers())
                .stateStoreDir("target/kafka-streams/" + Instant.now().getEpochSecond())
                .spansTopic("topic")
                .build();
        Thread.sleep(1000);
        Span root = Span.newBuilder().traceId("a").id("a").timestamp(TODAY).duration(10).build();
        storage.spanConsumer().accept(Collections.singletonList(root)).enqueue(new Callback<Void>() {
            @Override
            public void onSuccess(Void value) {
                assertTrue(true);
            }

            @Override
            public void onError(Throwable t) {
                fail("Error consuming span");
            }
        });

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase());
        KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singletonList("topic"));
        ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(Duration.ofSeconds(5));
        assertEquals(1, records.count());
    }

    @Test
    public void should_get_trace() throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        AdminClient adminClient = AdminClient.create(props);
        adminClient.createTopics(Collections.singletonList(new NewTopic("topic", 1, (short) 1))).all().get();

        StorageComponent storage = new KafkaStorage.Builder().bootstrapServers(kafka.getBootstrapServers())
                .stateStoreDir("target/kafka-streams/" + Instant.now().getEpochSecond())
                .spansTopic("topic")
                .build();
        Thread.sleep(3000);
        Span root = Span.newBuilder().traceId("a").id("a").timestamp(TODAY).duration(10).build();
        Span child = Span.newBuilder().traceId("a").id("b").timestamp(TODAY).duration(2).build();
        storage.spanConsumer().accept(Arrays.asList(root, child)).execute();
        Thread.sleep(10000);
        List<Span> result = storage.spanStore().getTrace("000000000000000a").execute();

        assertEquals(2, result.size());
        storage.close();
    }

    @Test
    public void should_get_service() throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        AdminClient adminClient = AdminClient.create(props);
        adminClient.createTopics(Collections.singletonList(new NewTopic("topic", 1, (short) 1))).all().get();

        StorageComponent storage = new KafkaStorage.Builder().bootstrapServers(kafka.getBootstrapServers())
                .stateStoreDir("target/kafka-streams/" + Instant.now().getEpochSecond())
                .spansTopic("topic")
                .build();
        Thread.sleep(3000);
        Span root = Span.newBuilder().traceId("a").id("a").localEndpoint(Endpoint.newBuilder().serviceName("service_a").build()).name("operation_a").timestamp(TODAY).duration(10).build();
        Span child = Span.newBuilder().traceId("a").id("b").localEndpoint(Endpoint.newBuilder().serviceName("service_a").build()).name("operation_b").timestamp(TODAY).duration(2).build();
        storage.spanConsumer().accept(Arrays.asList(root, child)).execute();
        Span root1 = Span.newBuilder().traceId("b").id("a").localEndpoint(Endpoint.newBuilder().serviceName("service_b").build()).name("operation_a").timestamp(TODAY).duration(10).build();
        Span child1 = Span.newBuilder().traceId("b").id("b").localEndpoint(Endpoint.newBuilder().serviceName("service_b").build()).name("operation_b").timestamp(TODAY).duration(2).build();
        storage.spanConsumer().accept(Arrays.asList(root1, child1)).execute();
        Thread.sleep(60000);
        List<String> result = storage.spanStore().getServiceNames().execute();

        assertEquals(2, result.size());

        List<String> spans = storage.spanStore().getSpanNames("service_a").execute();

        assertEquals(2, spans.size());

        storage.close();
    }

}