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

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import zipkin2.autoconfigure.storage.kafka.Access;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ZipkinKafkaStorageAutoConfigurationTest {
  AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

  @AfterEach void close() {
    context.close();
  }

  @Test void doesNotProvidesStorageComponent_whenStorageTypeNotKafka() {
    TestPropertyValues.of("zipkin.storage.type:elasticsearch").applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThatThrownBy(() -> context.getBean(KafkaStorage.class))
        .isInstanceOf(NoSuchBeanDefinitionException.class);
  }

  @Test void providesStorageComponent_whenStorageTypeKafka() {
    TestPropertyValues.of(
        "zipkin.storage.type:kafka"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class)).isNotNull();
  }

  @Test void canOverridesProperty_bootstrapServers() {
    TestPropertyValues.of(
        "zipkin.storage.type:kafka",
        "zipkin.storage.kafka.bootstrap-servers:host1:19092"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class).producerConfig.get(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo("host1:19092");
  }

  @Test void canOverridesProperty_adminConfigs() {
    TestPropertyValues.of(
        "zipkin.storage.type:kafka",
        "zipkin.storage.kafka.admin-overrides.bootstrap.servers:host1:19092"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class).adminConfig.get(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo("host1:19092");
  }

  @Test void canOverridesProperty_producerConfigs() {
    TestPropertyValues.of(
        "zipkin.storage.type:kafka",
        "zipkin.storage.kafka.producer-overrides.acks:1"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class).producerConfig.get(
        ProducerConfig.ACKS_CONFIG)).isEqualTo("1");
  }

  @Test void canOverridesProperty_aggregationStreamConfigs() {
    TestPropertyValues.of(
        "zipkin.storage.type:kafka",
        "zipkin.storage.kafka.aggregation-stream-overrides.application.id:agg1"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class).aggregationStreamConfig.get(
        StreamsConfig.APPLICATION_ID_CONFIG)).isEqualTo("agg1");
  }

  @Test void canOverridesProperty_traceStoreStreamConfigs() {
    TestPropertyValues.of(
        "zipkin.storage.type:kafka",
        "zipkin.storage.kafka.trace-store-stream-overrides.application.id:store1"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class).traceStoreStreamConfig.get(
        StreamsConfig.APPLICATION_ID_CONFIG)).isEqualTo("store1");
  }

  @Test void canOverridesProperty_dependencyStoreStreamConfigs() {
    TestPropertyValues.of(
        "zipkin.storage.type:kafka",
        "zipkin.storage.kafka.dependency-store-stream-overrides.application.id:store1"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class).dependencyStoreStreamConfig.get(
        StreamsConfig.APPLICATION_ID_CONFIG)).isEqualTo("store1");
  }

  @Test void canOverridesProperty_storeDirectory() {
    TestPropertyValues.of(
        "zipkin.storage.type:kafka",
        "zipkin.storage.kafka.storage-dir:/zipkin"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class).storageDir).isEqualTo("/zipkin");
  }

  @Test void canOverridesProperty_spansTopicName() {
    TestPropertyValues.of(
        "zipkin.storage.type:kafka",
        "zipkin.storage.kafka.spans-topic:zipkin-spans-1"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class).spansTopicName).isEqualTo("zipkin-spans-1");
  }

  @Test void canOverridesProperty_tracesTopicName() {
    TestPropertyValues.of(
        "zipkin.storage.type:kafka",
        "zipkin.storage.kafka.trace-topic:zipkin-traces-1"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class).traceTopicName).isEqualTo("zipkin-traces-1");
  }

  @Test void canOverridesProperty_dependenciesTopicName() {
    TestPropertyValues.of(
        "zipkin.storage.type:kafka",
        "zipkin.storage.kafka.dependency-topic:zipkin-dependencies-1"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class).dependencyTopicName).isEqualTo(
        "zipkin-dependencies-1");
  }

  @Test void canOverridesProperty_hostname() {
    TestPropertyValues.of(
      "zipkin.storage.type:kafka",
      "zipkin.storage.kafka.hostname:other_host"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class).hostname).isEqualTo(
      "other_host");
  }
}
