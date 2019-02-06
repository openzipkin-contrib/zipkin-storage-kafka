package zipkin2.storage.kafka;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.QueryableStoreTypes;
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

    final String traceStoreName;
    final String serviceStoreName;
    final String dependencyStoreName;
    final KafkaStreams kafkaStreams;

    KafkaSpanStore(KafkaStorage storage) {
        kafkaStreams = storage.kafkaStreams;
        traceStoreName = storage.traceStoreName;
        serviceStoreName = storage.serviceStoreName;
        dependencyStoreName = storage.dependencyStoreName;
    }

    @Override
    public Call<List<List<Span>>> getTraces(QueryRequest request) {
        //TODO implement get traces
        return Call.emptyList();
    }

    @Override
    public Call<List<Span>> getTrace(String traceId) {
        ReadOnlyKeyValueStore<String, byte[]> traceStore = kafkaStreams.store(traceStoreName, QueryableStoreTypes.keyValueStore());
        return new GetTraceProto3Call(traceStore, traceId);
    }

    static class GetTraceProto3Call extends KafkaStreamsStoreCall<List<Span>> {
        final String key;

        GetTraceProto3Call(ReadOnlyKeyValueStore<String, byte[]> store, String key) {
            super(store);
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
            return new GetTraceProto3Call(store, key);
        }

    }

    @Override
    public Call<List<String>> getServiceNames() {
        ReadOnlyKeyValueStore<String, byte[]> serviceStore = kafkaStreams.store(serviceStoreName, QueryableStoreTypes.keyValueStore());
        return new GetServiceNamesCall(serviceStore);
    }

    static class GetServiceNamesCall extends KafkaStreamsStoreCall<List<String>> {
        GetServiceNamesCall(ReadOnlyKeyValueStore<String, byte[]> store) {
            super(store);
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
            return new GetServiceNamesCall(store);
        }
    }

    @Override
    public Call<List<String>> getSpanNames(String serviceName) {
        ReadOnlyKeyValueStore<String, byte[]> serviceStore = kafkaStreams.store(serviceStoreName, QueryableStoreTypes.keyValueStore());
        return new GetSpanNamesCall(serviceStore, serviceName);
    }

    static class GetSpanNamesCall extends KafkaStreamsStoreCall<List<String>> {
        final String serviceName;

        GetSpanNamesCall(ReadOnlyKeyValueStore<String, byte[]> store, String serviceName) {
            super(store);
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
            return new GetSpanNamesCall(store, serviceName);
        }
    }

    @Override
    public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
        ReadOnlyKeyValueStore<String, byte[]> dependencyStore = kafkaStreams.store(dependencyStoreName, QueryableStoreTypes.keyValueStore());
        return new GetDependenciesJsonCall(dependencyStore);
    }

    static class GetDependenciesJsonCall extends KafkaStreamsStoreCall<List<DependencyLink>> {

        GetDependenciesJsonCall(ReadOnlyKeyValueStore<String, byte[]> store) {
            super(store);
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
            return new GetDependenciesJsonCall(store);
        }
    }
}
