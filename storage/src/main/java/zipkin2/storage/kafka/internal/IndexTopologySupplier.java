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
package zipkin2.storage.kafka.internal;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import zipkin2.Span;
import zipkin2.storage.kafka.internal.serdes.SpanNamesSerde;
import zipkin2.storage.kafka.internal.serdes.SpansSerde;
import zipkin2.storage.kafka.internal.stores.IndexStateStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class IndexTopologySupplier implements Supplier<Topology> {

    final String traceStoreName;
    final String indexDirectory;
    final String indexStoreName;
    final SpansSerde spansSerde;
    final SpanNamesSerde spanNamesSerde;

    public IndexTopologySupplier(String traceStoreName, String indexStoreName, String indexDirectory) {
        this.traceStoreName = traceStoreName;
        this.indexStoreName = indexStoreName;
        this.indexDirectory = indexDirectory;

        spansSerde = new SpansSerde();
        spanNamesSerde = new SpanNamesSerde();
    }

    @Override
    public Topology get() {
        IndexStateStore.Builder indexStoreBuilder = IndexStateStore.builder(indexStoreName);
        if (indexDirectory != null) {
            indexStoreBuilder.persistent(indexDirectory);
        }

        StreamsBuilder builder = new StreamsBuilder();

        builder.addGlobalStore(
                indexStoreBuilder,
                traceStoreName,
                Consumed.with(Serdes.String(), spansSerde),
                () -> new
                        Processor() {
                            IndexStateStore lucene;

                            @Override
                            public void init(ProcessorContext context) {
                                lucene = (IndexStateStore) context.getStateStore(indexStoreName);
                            }

                            @Override
                            public void process(Object key, Object value) {
                                List<Document> docs = new ArrayList<>();
                                List spans = (ArrayList) value;
                                for (Object s : spans) {
                                    Span span = (Span) s;
                                    String kind = span.kind() != null ? span.kind().name() : "";
                                    Document doc = new Document();
                                    doc.add(new StringField("trace_id", span.traceId(), Field.Store.YES));
                                    doc.add(new StringField("id", span.id(), Field.Store.YES));
                                    doc.add(new StringField("kind", kind, Field.Store.YES));
                                    String localServiceName = span.localServiceName() != null ? span.localServiceName() : "";
                                    doc.add(new StringField("local_service_name", localServiceName, Field.Store.YES));
                                    String remoteServiceName = span.remoteServiceName() != null ? span.remoteServiceName() : "";
                                    doc.add(new StringField("remote_service_name", remoteServiceName, Field.Store.YES));
                                    String name = span.name() != null ? span.name() : "";
                                    doc.add(new StringField("name", name, Field.Store.YES));
                                    long micros = span.timestampAsLong();
                                    doc.add(new LongPoint("ts", micros));
                                    doc.add(new LongPoint("duration", span.durationAsLong()));
                                    for (Map.Entry<String, String> tag : span.tags().entrySet()) {
                                        doc.add(new StringField(tag.getKey(), tag.getValue(), Field.Store.YES));
                                    }
                                    docs.add(doc);
                                }
                                lucene.put(docs);
                            }

                            @Override
                            public void close() {
                                lucene.close();
                            }
                        });
        return builder.build();
    }
}
