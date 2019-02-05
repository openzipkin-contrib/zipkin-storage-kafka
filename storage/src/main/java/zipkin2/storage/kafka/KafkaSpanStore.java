package zipkin2.storage.kafka;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import zipkin2.Call;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.codec.DependencyLinkBytesDecoder;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.SpanStore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class KafkaSpanStore implements SpanStore {

    final KafkaStreams kafkaStreams;
    final KeyValueBytesStoreSupplier traceStoreName;
    final KeyValueBytesStoreSupplier serviceSpanStoreName;
    final KeyValueBytesStoreSupplier dependencyStoreName;

    KafkaSpanStore(KafkaStorage storage) {
        this.kafkaStreams = storage.kafkaStreams;
        this.traceStoreName = storage.traceStoreSupplier;
        this.serviceSpanStoreName = storage.serviceSpanStoreSupplier;
        this.dependencyStoreName = storage.dependencyStoreSupplier;
    }

    @Override
    public Call<List<List<Span>>> getTraces(QueryRequest request) {
        //TODO implement get traces
        return Call.emptyList();
    }

    @Override
    public Call<List<Span>> getTrace(String traceId) {
        return new GetTraceProto3Call(kafkaStreams, traceStoreName.name(), traceId);
    }

    static class GetTraceProto3Call extends KafkaStreamsStoreCall<List<Span>> {
        final String tracesStoreName;
        final String key;

        GetTraceProto3Call(KafkaStreams kafkaStreams, String tracesStoreName, String key) {
            super(kafkaStreams, tracesStoreName);
            this.tracesStoreName = tracesStoreName;
            this.key = key;
        }

        @Override
        Function<ReadOnlyKeyValueStore<String, byte[]>, List<Span>> query() {
            return store -> {
                byte[] value = store.get(key);
                return SpanBytesDecoder.PROTO3.decodeList(value);
            };
        }

        @Override
        public Call<List<Span>> clone() {
            return new GetTraceProto3Call(kafkaStreams, storeName, key);
        }

    }

    @Override
    public Call<List<String>> getServiceNames() {
        return new GetServiceNamesCall(kafkaStreams, serviceSpanStoreName.name());
    }

    static class GetServiceNamesCall extends KafkaStreamsStoreCall<List<String>> {
        GetServiceNamesCall(KafkaStreams kafkaStreams, String storeName) {
            super(kafkaStreams, storeName);
        }

        @Override
        Function<ReadOnlyKeyValueStore<String, byte[]>, List<String>> query() {
            return store -> {
                List<String> keys = new ArrayList<>();
                store.all().forEachRemaining(keyValue -> keys.add(keyValue.key));
                return keys;
            };
        }

        @Override
        public Call<List<String>> clone() {
            return new GetServiceNamesCall(kafkaStreams, storeName);
        }
    }

    @Override
    public Call<List<String>> getSpanNames(String serviceName) {
        return new GetSpanNamesCall(kafkaStreams, serviceSpanStoreName.name(), serviceName);
    }

    static class GetSpanNamesCall extends KafkaStreamsStoreCall<List<String>> {
        final String serviceName;

        GetSpanNamesCall(KafkaStreams kafkaStreams, String storeName, String serviceName) {
            super(kafkaStreams, storeName);
            this.serviceName = serviceName;
        }

        @Override
        Function<ReadOnlyKeyValueStore<String, byte[]>, List<String>> query() {
            return store -> {
                byte[] operationNamesBytes = store.get(serviceName);
                String operationNamesText = new String(operationNamesBytes);
                return Arrays.asList(operationNamesText.split(","));
            };
        }

        @Override
        public Call<List<String>> clone() {
            return new GetSpanNamesCall(kafkaStreams, storeName, serviceName);
        }
    }

    @Override
    public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
        return new GetDependenciesJsonCall(kafkaStreams, dependencyStoreName.name());
    }

    static class GetDependenciesJsonCall extends KafkaStreamsStoreCall<List<DependencyLink>> {

        GetDependenciesJsonCall(KafkaStreams kafkaStreams, String storeName) {
            super(kafkaStreams, storeName);
        }

        @Override
        Function<ReadOnlyKeyValueStore<String, byte[]>, List<DependencyLink>> query() {
            return store -> {
                List<DependencyLink> dependencyLinks = new ArrayList<>();
                store.all().forEachRemaining(keyValue ->
                        dependencyLinks.addAll(DependencyLinkBytesDecoder.JSON_V1.decodeList(keyValue.value)));
                return dependencyLinks;
            };
        }

        @Override
        public Call<List<DependencyLink>> clone() {
            return new GetDependenciesJsonCall(kafkaStreams, storeName);
        }
    }
}
