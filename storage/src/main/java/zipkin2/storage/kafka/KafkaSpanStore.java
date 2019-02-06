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

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import zipkin2.Call;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.SpanStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

public class KafkaSpanStore implements SpanStore {

    final String traceStoreName;
    final String serviceStoreName;
    final String dependencyStoreName;

    final KafkaStreams kafkaStreams;

    final ReadOnlyKeyValueStore<String, Set<String>> serviceStore;
    final ReadOnlyKeyValueStore<String, List<Span>> traceStore;
    final ReadOnlyKeyValueStore<String, List<DependencyLink>> dependencyStore;

    KafkaSpanStore(KafkaStorage storage) {
        kafkaStreams = storage.kafkaStreams;
        traceStoreName = storage.traceStoreName;
        serviceStoreName = storage.serviceStoreName;
        dependencyStoreName = storage.dependencyStoreName;
        serviceStore = kafkaStreams.store(serviceStoreName, QueryableStoreTypes.keyValueStore());
        traceStore = kafkaStreams.store(traceStoreName, QueryableStoreTypes.keyValueStore());
        dependencyStore = kafkaStreams.store(dependencyStoreName, QueryableStoreTypes.keyValueStore());
    }

    @Override
    public Call<List<List<Span>>> getTraces(QueryRequest request) {
        //TODO implement get traces
        return Call.emptyList();
    }

    @Override
    public Call<List<Span>> getTrace(String traceId) {
        return new GetTraceProto3Call(traceStore, traceId);
    }

    static class GetTraceProto3Call extends KafkaStreamsStoreCall<String, List<Span>, List<Span>> {
        final String key;

        GetTraceProto3Call(ReadOnlyKeyValueStore<String, List<Span>> store, String key) {
            super(store);
            this.key = key;
        }

        @Override
        Function<ReadOnlyKeyValueStore<String, List<Span>>, List<Span>> query() {
            return store -> {
                store.all().forEachRemaining(System.out::println);
                return store.get(key);
            };
        }

        @Override
        public Call<List<Span>> clone() {
            return new GetTraceProto3Call(store, key);
        }

    }

    @Override
    public Call<List<String>> getServiceNames() {
        return new GetServiceNamesCall(serviceStore);
    }

    static class GetServiceNamesCall extends KafkaStreamsStoreCall<String, Set<String>, List<String>> {
        GetServiceNamesCall(ReadOnlyKeyValueStore<String, Set<String>> store) {
            super(store);
        }

        @Override
        Function<ReadOnlyKeyValueStore<String, Set<String>>, List<String>> query() {
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
        return new GetSpanNamesCall(serviceStore, serviceName);
    }

    static class GetSpanNamesCall extends KafkaStreamsStoreCall<String, Set<String>, List<String>> {
        final String serviceName;

        GetSpanNamesCall(ReadOnlyKeyValueStore<String, Set<String>> store, String serviceName) {
            super(store);
            this.serviceName = serviceName;
        }

        @Override
        Function<ReadOnlyKeyValueStore<String, Set<String>>, List<String>> query() {
            return store -> {
                Set<String> spanNames = store.get(serviceName);
                return new ArrayList<>(spanNames);
            };
        }

        @Override
        public Call<List<String>> clone() {
            return new GetSpanNamesCall(store, serviceName);
        }
    }

    @Override
    public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
        return new GetDependenciesJsonCall(dependencyStore);
    }

    static class GetDependenciesJsonCall extends KafkaStreamsStoreCall<String, List<DependencyLink>, List<DependencyLink>> {

        GetDependenciesJsonCall(ReadOnlyKeyValueStore<String, List<DependencyLink>> store) {
            super(store);
        }

        @Override
        Function<ReadOnlyKeyValueStore<String, List<DependencyLink>>, List<DependencyLink>> query() {
            return store -> {
                List<DependencyLink> dependencyLinks = new ArrayList<>();
                store.all().forEachRemaining(dependencyLink -> dependencyLinks.addAll(dependencyLink.value));
                return dependencyLinks;
            };
        }

        @Override
        public Call<List<DependencyLink>> clone() {
            return new GetDependenciesJsonCall(store);
        }
    }
}
