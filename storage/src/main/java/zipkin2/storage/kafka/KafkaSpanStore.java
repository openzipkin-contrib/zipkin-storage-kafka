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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.SpanStore;
import zipkin2.storage.kafka.internal.stores.IndexStateStore;
import zipkin2.storage.kafka.internal.stores.IndexStoreType;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KafkaSpanStore implements SpanStore {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpanStore.class);

    final KafkaStreams kafkaStreams;

    final ReadOnlyKeyValueStore<String, Set<String>> serviceStore;
    final ReadOnlyKeyValueStore<String, List<Span>> traceStore;
    final ReadOnlyKeyValueStore<String, DependencyLink> dependencyStore;
    final String indexStoreName;
    final KafkaStreams luceneKafkaStreams;

    KafkaSpanStore(KafkaStorage storage) {
        kafkaStreams = storage.processKafkaStreams;
        traceStore = kafkaStreams.store(storage.traceStoreName, QueryableStoreTypes.keyValueStore());
        serviceStore = kafkaStreams.store(storage.serviceStoreName, QueryableStoreTypes.keyValueStore());
        dependencyStore = kafkaStreams.store(storage.dependencyStoreName, QueryableStoreTypes.keyValueStore());
        indexStoreName = storage.indexStoreName;
        luceneKafkaStreams = storage.indexKafkaStreams;
    }

    @Override
    public Call<List<List<Span>>> getTraces(QueryRequest request) {
        return new GetTracesCall(luceneKafkaStreams, request, traceStore, indexStoreName);
    }

    static class GetTracesCall extends Call.Base<List<List<Span>>> {
        final KafkaStreams kafkaStreams;
        final String indexStoreName;
        final Directory directory;
        final QueryRequest queryRequest;
        final ReadOnlyKeyValueStore<String, List<Span>> traceStore;

        GetTracesCall(KafkaStreams kafkaStreams,
                      QueryRequest queryRequest,
                      ReadOnlyKeyValueStore<String, List<Span>> traceStore,
                      String indexStoreName) {
            this.kafkaStreams = kafkaStreams;
            this.indexStoreName = indexStoreName;
            IndexStateStore lucene = kafkaStreams.store(indexStoreName, new IndexStoreType());
            this.directory = lucene.directory();
            this.queryRequest = queryRequest;
            this.traceStore = traceStore;
        }

        @Override
        protected List<List<Span>> doExecute() throws IOException {
            return query();
        }

        private List<List<Span>> query() throws IOException {
            IndexReader reader = DirectoryReader.open(directory);
            IndexSearcher indexSearcher = new IndexSearcher(reader);

            BooleanQuery.Builder builder = new BooleanQuery.Builder();

            if (queryRequest.serviceName() != null) {
                String serviceName = queryRequest.serviceName();
                TermQuery serviceNameQuery = new TermQuery(new Term("local_service_name", serviceName));
                builder.add(serviceNameQuery, BooleanClause.Occur.MUST);
            }

            if (queryRequest.spanName() != null) {
                String spanName = queryRequest.spanName();
                TermQuery spanNameQuery = new TermQuery(new Term("name", spanName));
                builder.add(spanNameQuery, BooleanClause.Occur.MUST);
            }

            for (Map.Entry<String, String> entry : queryRequest.annotationQuery().entrySet()) {
                TermQuery spanNameQuery = new TermQuery(new Term(entry.getKey(), entry.getValue()));
                builder.add(spanNameQuery, BooleanClause.Occur.MUST);
            }

            if (queryRequest.maxDuration() != null) {
                builder.add(LongPoint.newRangeQuery(
                        "duration",
                        queryRequest.minDuration(),
                        queryRequest.maxDuration()),
                        BooleanClause.Occur.MUST);
            }

            long start = queryRequest.endTs() - queryRequest.lookback();
            long end = queryRequest.endTs();
            builder.add(LongPoint.newRangeQuery(
                    "ts", new Long(start + "000"), new Long(end + "000")), BooleanClause.Occur.MUST);

            int total = queryRequest.limit();
            Sort sort = Sort.RELEVANCE;

            Set<String> traceIds = new HashSet<>();

            BooleanQuery query = builder.build();
            TopFieldDocs docs = indexSearcher.search(query, total, sort);

            LOG.debug("Total results of query {}: {}", query, docs.totalHits);

            for (ScoreDoc doc : docs.scoreDocs) {
                Document document = indexSearcher.doc(doc.doc);
                String traceId = document.get("trace_id");
                traceIds.add(traceId);
            }

            reader.close();

            return traceIds.stream()
                    .map(traceStore::get)
                    .collect(Collectors.toList());
        }

        @Override
        protected void doEnqueue(Callback<List<List<Span>>> callback) {
            try {
                callback.onSuccess(query());
            } catch (Exception e) {
                callback.onError(e);
            }
        }

        @Override
        public Call<List<List<Span>>> clone() {
            return new GetTracesCall(kafkaStreams, queryRequest, traceStore, indexStoreName);
        }
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
                if (serviceName == null || serviceName.equals("all")) return new ArrayList<>();
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
