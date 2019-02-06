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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.CheckResult;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StorageComponent;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class KafkaStorage extends StorageComponent {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaStorage.class);

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder extends StorageComponent.Builder {
        String bootstrapServers = "localhost:29092";
        String applicationId = "zipkin-server_v2";
        String traceStoreName = "zipkin-traces-store";
        String serviceStoreName = "zipkin-service-operations-store";
        String dependencyStoreName = "zipkin-dependencies-store";
        String stateStoreDir = "/tmp/kafka-streams";

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
            if (bootstrapServers == null) throw new NullPointerException("bootstrapServers == null");
            this.bootstrapServers = bootstrapServers;
            return this;
        }

        public Builder applicationId(String applicationId) {
            if (applicationId == null) throw new NullPointerException("applicationId == null");
            this.applicationId = applicationId;
            return this;
        }

        public Builder tracesStoreName(String tracesStoreName) {
            if (tracesStoreName == null) throw new NullPointerException("traceStoreName == null");
            this.traceStoreName = tracesStoreName;
            return this;
        }

        public Builder serviceOperationsStoreName(String serviceOperationsStoreName) {
            if (serviceOperationsStoreName == null)
                throw new NullPointerException("serviceStoreName == null");
            this.serviceStoreName = serviceOperationsStoreName;
            return this;
        }

        public Builder dependenciesStoreName(String dependenciesStoreName) {
            if (dependenciesStoreName == null) throw new NullPointerException("dependencyStoreName == null");
            this.dependencyStoreName = dependenciesStoreName;
            return this;
        }

        public Builder stateStoreDir(String stateStoreDir) {
            if (stateStoreDir == null) throw new NullPointerException("stateStoreDir == null");
            this.stateStoreDir = stateStoreDir;
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
    final KafkaStreams kafkaStreams;
    final String traceStoreName;
    final String serviceStoreName;
    final String dependencyStoreName;

    KafkaStorage(Builder builder) {
        final Properties producerConfigs = new Properties();
        producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, builder.bootstrapServers);
        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class); //TODO validate format
        producerConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        //TODO add a way to introduce custom properties
        this.producer = new KafkaProducer<>(producerConfigs);

        StoreBuilder<KeyValueStore<String, byte[]>> traceStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(builder.traceStoreName),
                Serdes.String(),
                Serdes.ByteArray());
        StoreBuilder<KeyValueStore<String, byte[]>> serviceStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(builder.serviceStoreName),
                Serdes.String(),
                Serdes.ByteArray());
        StoreBuilder<KeyValueStore<String, byte[]>> dependencyStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(builder.dependencyStoreName),
                Serdes.String(),
                Serdes.ByteArray());

        final Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, builder.bootstrapServers);
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, builder.applicationId);
        streamsConfig.put(StreamsConfig.EXACTLY_ONCE, true);
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, builder.stateStoreDir);

        final Topology topology = new TopologySupplier(
                traceStoreBuilder.name(), serviceStoreBuilder.name(), dependencyStoreBuilder.name()).get();

        this.kafkaStreams = new KafkaStreams(topology, streamsConfig);

        this.traceStoreName = builder.traceStoreName;
        this.serviceStoreName = builder.serviceStoreName;
        this.dependencyStoreName =builder.dependencyStoreName;

        new KafkaStreamsWorker(kafkaStreams).get();
//        this.traceStore = traceStoreBuilder.withCachingEnabled().build();
//        this.serviceStore = serviceStoreBuilder.withCachingEnabled().build();
//        this.dependencyStore = dependencyStoreBuilder.withCachingEnabled().build();
    }

    @Override
    public SpanStore spanStore() {
        return new KafkaSpanStore(this);
    }

    @Override
    public SpanConsumer spanConsumer() {
        return new KafkaSpanConsumer(this);
    }

    @Override
    public void close() {
        producer.close(1, TimeUnit.SECONDS);
        kafkaStreams.close(Duration.ofSeconds(1));
    }

    public static final class KafkaStreamsWorker {
        volatile ExecutorService pool;
        final KafkaStreams kafkaStreams;
        final AtomicReference<CheckResult> failure = new AtomicReference<>();

        KafkaStreamsWorker(KafkaStreams kafkaStreams) {
            this.kafkaStreams = kafkaStreams;
        }

        ExecutorService get() {
            if (pool == null) {
                synchronized (this) {
                    if (pool == null) {
                        pool = compute();
                    }
                }
            }
            return pool;
        }

        void close() {
            ExecutorService maybePool = pool;
            if (maybePool == null) return;
            try {
                maybePool.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {

            }
        }

        private ExecutorService compute() {
            ExecutorService pool = Executors.newSingleThreadExecutor();
            pool.execute(guardFailures(kafkaStreams));
            return pool;
        }

        private Runnable guardFailures(KafkaStreams kafkaStreams) {
            return () -> {
                try {
                    kafkaStreams.start();
                } catch (Exception e) {
                    LOG.error("Kafka Streams worker exited with error", e);
                    failure.set(CheckResult.failed(e));
                }
            };
        }
    }
}
