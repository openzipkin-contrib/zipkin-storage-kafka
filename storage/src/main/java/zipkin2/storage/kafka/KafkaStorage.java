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
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.CheckResult;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StorageComponent;
import zipkin2.storage.kafka.internal.IndexTopologySupplier;
import zipkin2.storage.kafka.internal.ProcessTopologySupplier;

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
        String spansTopic = "zipkin-spans_v1";
        String applicationId = "zipkin-server_v1";
        String traceStoreName = "zipkin-traces-store_v1";
        String serviceStoreName = "zipkin-service-operations-store_v1";
        String dependencyStoreName = "zipkin-dependencies-store_v1";
        String indexStoreName = "zipkin-index-store_v1";
        String stateStoreDir = "/tmp/kafka-streams";
        String indexDirectory = "/tmp/lucene-index";

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

        public Builder spansTopic(String spansTopic) {
            if (spansTopic == null) throw new NullPointerException("spansTopic == null");
            this.spansTopic = spansTopic;
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

        public Builder indexStoreName(String indexStoreName) {
            if (indexStoreName == null) throw new NullPointerException("indexStoreName == null");
            this.indexStoreName = indexStoreName;
            return this;
        }

        public Builder stateStoreDir(String stateStoreDir) {
            if (stateStoreDir == null) throw new NullPointerException("stateStoreDir == null");
            this.stateStoreDir = stateStoreDir;
            return this;
        }

        public Builder indexDirectory(String indexDirectory) {
            if (indexDirectory == null) throw new NullPointerException("indexDirectory == null");
            this.indexDirectory = indexDirectory;
            return this;
        }

        @Override
        public StorageComponent build() {
                return new KafkaStorage(this);
        }

        Builder() {
        }
    }

    final Properties producerConfigs;
    final String spansTopic;

    final Properties processStreamsConfig;
    final Properties indexStreamsConfig;
    final Topology processTopology;
    final Topology indexTopology;

    Producer<String, byte[]> producer;
    KafkaStreamsWorker processStreamsWorker;
    KafkaStreams processStreams;
    KafkaStreamsWorker indexStreamsWorker;
    KafkaStreams indexStreams;

    final String traceStoreName;
    final String serviceStoreName;
    final String dependencyStoreName;
    final String indexStoreName;

    KafkaStorage(Builder builder) {
        this.traceStoreName = builder.traceStoreName;
        this.serviceStoreName = builder.serviceStoreName;
        this.dependencyStoreName = builder.dependencyStoreName;
        this.indexStoreName = builder.indexStoreName;
        this.spansTopic = builder.spansTopic;

        producerConfigs = new Properties();
        producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, builder.bootstrapServers);
        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        producerConfigs.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, CompressionType.ZSTD.name);

        processStreamsConfig = new Properties();
        processStreamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, builder.bootstrapServers);
        processStreamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        processStreamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);
        processStreamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, builder.applicationId);
        processStreamsConfig.put(StreamsConfig.EXACTLY_ONCE, true);
        processStreamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, builder.stateStoreDir);
        processStreamsConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, CompressionType.ZSTD.name);

        processTopology = new ProcessTopologySupplier(spansTopic, traceStoreName, serviceStoreName, dependencyStoreName).get();

        indexStreamsConfig = new Properties();
        indexStreamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, builder.bootstrapServers);
        indexStreamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        indexStreamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);
        indexStreamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "lucene_" + builder.applicationId);
        indexStreamsConfig.put(StreamsConfig.EXACTLY_ONCE, true);
        indexStreamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, builder.stateStoreDir + "_lucene");

        indexTopology = new IndexTopologySupplier(traceStoreName, indexStoreName, builder.indexDirectory).get();
    }

    volatile boolean closeCalled, connected;

    void connect() {
        if (closeCalled) throw new IllegalStateException("closed");
        if (!connected) {
            // blocking to prevent access while initializing
            synchronized (this) {
                if (closeCalled) throw new IllegalStateException("closed");
                if (!connected) {
                    connectConsumer();
                    connectStore();
                    connected = true;
                }
            }
        }
    }

    void connectConsumer() {
        producer = new KafkaProducer<>(producerConfigs);
    }

    void connectStore() {
        processStreams = new KafkaStreams(processTopology, processStreamsConfig);
        processStreamsWorker = new KafkaStreamsWorker(processStreams);
        processStreamsWorker.get();

        indexStreams = new KafkaStreams(indexTopology, indexStreamsConfig);
        indexStreamsWorker = new KafkaStreamsWorker(indexStreams);
        indexStreamsWorker.get();
    }

    @Override
    public SpanStore spanStore() {
        connect();
        return new KafkaSpanStore(this);
    }

    @Override
    public SpanConsumer spanConsumer() {
        connect();
        return new KafkaSpanConsumer(this);
    }

    @Override
    public void close() {
        if (closeCalled) return;
        // blocking to prevent access while initializing
        synchronized (this) {
            if (!closeCalled) {
                doClose();
                closeCalled = true;
            }
        }

    }

    void doClose() {
        try {
            producer.flush();
            producer.close(1, TimeUnit.SECONDS);
            processStreams.close(Duration.ofSeconds(1));
            indexStreams.close(Duration.ofSeconds(1));
            processStreamsWorker.close();
            indexStreamsWorker.close();
        } catch (Exception | Error e) {
            LOG.warn("error closing client {}", e.getMessage(), e);
        }
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
                LOG.error("Error waiting for stream poll to close", e);
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
