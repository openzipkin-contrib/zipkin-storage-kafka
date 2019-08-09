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

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.record.CompressionType;
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
        "zipkin.storage.type:kafkastore"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class)).isNotNull();
  }

  @Test
  public void canOverridesProperty_bootstrapServers() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
        "zipkin.storage.type:kafkastore",
        "zipkin.storage.kafka.bootstrap-servers:host1:19092"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class).producerConfig.get(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo("host1:19092");
  }

  @Test
  public void canOverridesProperty_ensureTopics() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
        "zipkin.storage.type:kafkastore",
        "zipkin.storage.kafka.ensure-topics:false"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class).ensureTopics).isEqualTo(false);
  }

  @Test
  public void canOverridesProperty_compressionType() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
        "zipkin.storage.type:kafkastore",
        "zipkin.storage.kafka.compression-type:SNAPPY"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class).producerConfig.get(
        ProducerConfig.COMPRESSION_TYPE_CONFIG)).isEqualTo(CompressionType.SNAPPY.name);
  }

  @Test
  public void canOverridesProperty_storeDirectory() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
        "zipkin.storage.type:kafkastore",
        "zipkin.storage.kafka.store-directory:/zipkin"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class).storageDirectory).isEqualTo("/zipkin");
  }

  @Test
  public void canOverridesProperty_spansTopicName() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
        "zipkin.storage.type:kafkastore",
        "zipkin.storage.kafka.spans-topic:zipkin-spans-1"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class).spansTopic.name).isEqualTo("zipkin-spans-1");
  }

  @Test
  public void canOverridesProperty_spansTopicPartitions() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
        "zipkin.storage.type:kafkastore",
        "zipkin.storage.kafka.spans-topic-partitions:2"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class).spansTopic.partitions).isEqualTo(2);
  }

  @Test
  public void canOverridesProperty_spansTopicReplicationFactor() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
        "zipkin.storage.type:kafkastore",
        "zipkin.storage.kafka.spans-topic-replication-factor:2"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class).spansTopic.replicationFactor).isEqualTo(
        (short) 2);
  }

  @Test
  public void canOverridesProperty_dependenciesTopicName() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
        "zipkin.storage.type:kafkastore",
        "zipkin.storage.kafka.dependency-links-topic:zipkin-dependencies-1"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class).dependencyLinksTopic.name).isEqualTo(
        "zipkin-dependencies-1");
  }

  @Test
  public void canOverridesProperty_dependenciesTopicPartitions() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
        "zipkin.storage.type:kafkastore",
        "zipkin.storage.kafka.dependency-links-topic-partitions:2"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(context.getBean(KafkaStorage.class).dependencyLinksTopic.partitions).isEqualTo(2);
  }

  @Test
  public void canOverridesProperty_dependenciesTopicReplicationFactor() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
        "zipkin.storage.type:kafkastore",
        "zipkin.storage.kafka.dependency-links-topic-replication-factor:2"
    ).applyTo(context);
    Access.registerKafka(context);
    context.refresh();

    assertThat(
        context.getBean(KafkaStorage.class).dependencyLinksTopic.replicationFactor).isEqualTo(
        (short) 2);
  }
}