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
import zipkin2.storage.kafka.internal.LuceneStateStore;
import zipkin2.storage.kafka.internal.SpanNamesSerde;
import zipkin2.storage.kafka.internal.SpansSerde;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class LuceneTopologySupplier implements Supplier<Topology> {

    final String traceStoreName;
    final String indexStoreName;

    final SpansSerde spansSerde = new SpansSerde();

    final SpanNamesSerde spanNamesSerde;

    public LuceneTopologySupplier(String traceStoreName,
                                  String indexStoreName) {
        this.traceStoreName = traceStoreName;
        this.indexStoreName = indexStoreName;

        spanNamesSerde = new SpanNamesSerde();
    }

    @Override
    public Topology get() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.addGlobalStore(
                new LuceneStateStore.LuceneStateStoreBuilder(indexStoreName).persistent()
                        .withIndexDirectory("/tmp/lucene-kafka-streams"),
                traceStoreName,
                Consumed.with(Serdes.String(), spansSerde),
                () -> new
                        Processor() {
                            LuceneStateStore lucene;

                            @Override
                            public void init(ProcessorContext context) {
                                lucene = (LuceneStateStore) context.getStateStore(indexStoreName);
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
//                                    doc.add(new StringField("parent_id", span.parentId(), Field.Store.YES));
                                    doc.add(new StringField("id", span.id(), Field.Store.YES));
                                    doc.add(new StringField("kind", kind, Field.Store.YES));
                                    String localServiceName = span.localServiceName() != null ? span.localServiceName() : "";
                                    doc.add(new StringField("local_service_name", localServiceName, Field.Store.YES));
                                    String remoteServiceName = span.remoteServiceName() != null ? span.remoteServiceName() : "";
                                    doc.add(new StringField("remote_service_name", remoteServiceName, Field.Store.YES));
                                    String name = span.name() != null ? span.name() : "";
                                    doc.add(new StringField("name", name, Field.Store.YES));
                                    doc.add(new LongPoint("ts", span.timestamp()));
//                                    doc.add(new LongPoint("duration", span.duration()));
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
