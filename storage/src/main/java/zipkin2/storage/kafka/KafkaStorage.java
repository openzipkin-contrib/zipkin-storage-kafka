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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StorageComponent;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaStorage extends StorageComponent {

    public static class Builder extends StorageComponent.Builder {
        String bootstrapServers = "localhost:29092";

        @Override
        public StorageComponent.Builder strictTraceId(boolean strictTraceId) {
            if (!strictTraceId) throw new IllegalArgumentException("unstrict trace ID not supported");
            return this;
        }

        @Override
        public StorageComponent.Builder searchEnabled(boolean searchEnabled) {
            if (searchEnabled) throw new IllegalArgumentException("search not supported");
            return this;
        }

        @Override
        public Builder autocompleteKeys(List<String> keys) {
            if (keys == null) throw new NullPointerException("keys == null");
            if (!keys.isEmpty()) throw new IllegalArgumentException("autocomplete not supported");
            return this;
        }

        public Builder bootstrapServers(String bootstrapServers) {
            if (bootstrapServers == null) throw new NullPointerException("keys == null");
            this.bootstrapServers = bootstrapServers;
            return this;
        }

        @Override
        public StorageComponent build() {
            return new KafkaStorage(this);
        }

        Builder() {
        }
    }

    final Producer<String, byte[]> producer;

    KafkaStorage(Builder builder) {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, builder.bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class); //TODO validate format
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        //TODO add a way to introduce custom properties
        this.producer = new KafkaProducer<>(properties);
    }

    @Override
    public SpanStore spanStore() {
        return null;
    }

    @Override
    public SpanConsumer spanConsumer() {
        return new KafkaSpanConsumer(this);
    }

    @Override
    public void close() {
        producer.close(1, TimeUnit.SECONDS);
    }
}
