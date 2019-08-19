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

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import zipkin2.autoconfigure.storage.kafka.Access;

import static org.assertj.core.api.Assertions.assertThat;

public class ZipkinKafkaStorageAutoConfigurationTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  AnnotationConfigApplicationContext context;

  @After
  public void close() {
    if (context != null) {
      context.close();
    }
  }

  @Test
  public void doesNotProvidesStorageComponent_whenStorageTypeNotKafka() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of("zipkin.storage.type:elasticsearch").applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    thrown.expect(NoSuchBeanDefinitionException.class);
    context.getBean(KafkaStorage.class);
  }

  @Test
  public void providesStorageComponent_whenStorageTypeKafka() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
        "zipkin.storage.type:kafka"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class)).isNotNull();
  }

  @Test
  public void canOverridesProperty_bootstrapServers() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
        "zipkin.storage.type:kafka",
        "zipkin.storage.kafka.bootstrap-servers:host1:19092"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class).producerConfig.get(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo("host1:19092");
  }

  @Test
  public void canOverridesProperty_adminConfigs() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
        "zipkin.storage.type:kafka",
        "zipkin.storage.kafka.admin-overrides.bootstrap.servers:host1:19092"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class).adminConfig.get(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo("host1:19092");
  }

  @Test
  public void canOverridesProperty_producerConfigs() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
        "zipkin.storage.type:kafka",
        "zipkin.storage.kafka.producer-overrides.acks:1"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class).producerConfig.get(
        ProducerConfig.ACKS_CONFIG)).isEqualTo("1");
  }
  @Test
  public void canOverridesProperty_aggregationStreamConfigs() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
        "zipkin.storage.type:kafka",
        "zipkin.storage.kafka.aggregation-stream-overrides.application.id:agg1"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class).aggregationStreamConfig.get(
        StreamsConfig.APPLICATION_ID_CONFIG)).isEqualTo("agg1");
  }

  @Test
  public void canOverridesProperty_traceStoreStreamConfigs() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
        "zipkin.storage.type:kafka",
        "zipkin.storage.kafka.trace-store-stream-overrides.application.id:store1"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class).traceStoreStreamConfig.get(
        StreamsConfig.APPLICATION_ID_CONFIG)).isEqualTo("store1");
  }

  @Test
  public void canOverridesProperty_dependencyStoreStreamConfigs() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
        "zipkin.storage.type:kafka",
        "zipkin.storage.kafka.dependency-store-stream-overrides.application.id:store1"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class).dependencyStoreStreamConfig.get(
        StreamsConfig.APPLICATION_ID_CONFIG)).isEqualTo("store1");
  }

  @Test
  public void canOverridesProperty_storeDirectory() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
        "zipkin.storage.type:kafka",
        "zipkin.storage.kafka.store-dir:/zipkin"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class).storageDirectory).isEqualTo("/zipkin");
  }

  @Test
  public void canOverridesProperty_spansTopicName() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
        "zipkin.storage.type:kafka",
        "zipkin.storage.kafka.span-topic:zipkin-spans-1"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class).spanTopicName).isEqualTo("zipkin-spans-1");
  }

  @Test
  public void canOverridesProperty_tracesTopicName() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
        "zipkin.storage.type:kafka",
        "zipkin.storage.kafka.trace-topic:zipkin-traces-1"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class).traceTopicName).isEqualTo("zipkin-traces-1");
  }

  @Test
  public void canOverridesProperty_dependenciesTopicName() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
        "zipkin.storage.type:kafka",
        "zipkin.storage.kafka.dependency-topic:zipkin-dependencies-1"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class).dependencyTopicName).isEqualTo(
        "zipkin-dependencies-1");
  }
}