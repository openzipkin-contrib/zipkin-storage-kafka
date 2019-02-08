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

import org.apache.kafka.clients.producer.ProducerConfig;
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
                "zipkin.storage.kafka.bootstrap_servers:host1:9092"
        ).applyTo(context);
        Access.registerKafka(context);
        context.refresh();

        assertThat(context.getBean(KafkaStorage.class).producerConfigs.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo("host1:9092");
    }
}