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

    final KafkaStreams kafkaStreams;

    final ReadOnlyKeyValueStore<String, Set<String>> serviceStore;
    final ReadOnlyKeyValueStore<String, List<Span>> traceStore;
    final ReadOnlyKeyValueStore<String, DependencyLink> dependencyStore;
//    final IndexSearcher indexSearcher;

    KafkaSpanStore(KafkaStorage storage) {
        kafkaStreams = storage.kafkaStreams;
        traceStore = kafkaStreams.store(storage.traceStoreName, QueryableStoreTypes.keyValueStore());
        serviceStore = kafkaStreams.store(storage.serviceStoreName, QueryableStoreTypes.keyValueStore());
        dependencyStore = kafkaStreams.store(storage.dependencyStoreName, QueryableStoreTypes.keyValueStore());
//        indexSearcher = storage.indexSearcher;
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
            return store -> store.get(key);
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

    static class GetDependenciesJsonCall extends KafkaStreamsStoreCall<String, DependencyLink, List<DependencyLink>> {

        GetDependenciesJsonCall(ReadOnlyKeyValueStore<String, DependencyLink> store) {
            super(store);
        }

        @Override
        Function<ReadOnlyKeyValueStore<String, DependencyLink>, List<DependencyLink>> query() {
            return store -> {
                List<DependencyLink> dependencyLinks = new ArrayList<>();
                store.all().forEachRemaining(dependencyLink -> dependencyLinks.add(dependencyLink.value));
                return dependencyLinks;
            };
        }

        @Override
        public Call<List<DependencyLink>> clone() {
            return new GetDependenciesJsonCall(store);
        }
    }
}
