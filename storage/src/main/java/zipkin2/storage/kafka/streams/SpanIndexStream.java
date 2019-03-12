/*
 * Copyright 2019 jeqo
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
package zipkin2.storage.kafka.streams;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import zipkin2.Annotation;
import zipkin2.Span;
import zipkin2.storage.kafka.streams.serdes.SpansSerde;
import zipkin2.storage.kafka.streams.stores.IndexStateStore;

/**
 * Streams processing supplier for full-text indexing.
 *
 * This module enable search capabilities by storing traces into a Lucene index.
 */
public class SpanIndexStream implements Supplier<Topology> {

  final String spansTopic;
  final String globalTracesIndexStoreName;

  final String indexDirectory;

  final SpansSerde spansSerde;

  public SpanIndexStream(
      String spansTopic,
      String globalTracesIndexStoreName,
      String indexDirectory) {
    this.spansTopic = spansTopic;
    this.globalTracesIndexStoreName = globalTracesIndexStoreName;
    this.indexDirectory = indexDirectory;

    spansSerde = new SpansSerde();
  }

  @Override
  public Topology get() {
    IndexStateStore.Builder indexStoreBuilder =
        IndexStateStore.builder(globalTracesIndexStoreName);
    indexStoreBuilder.persistent(indexDirectory);

    StreamsBuilder builder = new StreamsBuilder();

    builder.addGlobalStore(
        indexStoreBuilder,
        spansTopic,
        Consumed.with(Serdes.String(), spansSerde),
        () -> new
            Processor<String, List<Span>>() {
              private IndexStateStore index;

              @Override
              public void init(ProcessorContext context) {
                index = (IndexStateStore) context.getStateStore(globalTracesIndexStoreName);
              }

              @Override
              public void process(String spanId, List<Span> spans) {
                if (spans == null) { // clean index when trace removed
                  TermQuery query = new TermQuery(new Term("trace_id", spanId));
                  index.delete(query);
                } else { // index spans
                  List<Document> docs = new ArrayList<>();
                  for (Span span : spans) {
                    String kind = span.kind() != null ? span.kind().name() : "";
                    Document doc = new Document();
                    doc.add(
                        new SortedDocValuesField("trace_id_sorted", new BytesRef(span.traceId())));
                    doc.add(new StringField("trace_id", span.traceId(), Field.Store.YES));
                    doc.add(new StringField("id", span.id(), Field.Store.YES));
                    doc.add(new StringField("kind", kind, Field.Store.YES));
                    String localServiceName =
                        span.localServiceName() != null ? span.localServiceName() : "";
                    doc.add(
                        new StringField("local_service_name", localServiceName, Field.Store.YES));
                    String remoteServiceName =
                        span.remoteServiceName() != null ? span.remoteServiceName() : "";
                    doc.add(
                        new StringField("remote_service_name", remoteServiceName, Field.Store.YES));
                    String name = span.name() != null ? span.name() : "";
                    doc.add(new StringField("name", name, Field.Store.YES));
                    long micros = span.timestampAsLong();
                    doc.add(new LongPoint("ts", micros));
                    doc.add(new NumericDocValuesField("ts_sorted", micros));
                    doc.add(new LongPoint("duration", span.durationAsLong()));
                    for (Map.Entry<String, String> tag : span.tags().entrySet()) {
                      doc.add(new TextField("tag", tag.getKey()+ ":" + tag.getValue(), Field.Store.YES));
                    }
                    for (Annotation annotation : span.annotations()) {
                      doc.add(new TextField("annotation", annotation.value(), Field.Store.YES));
                    }
                    for (Annotation annotation : span.annotations()) {
                      doc.add(new TextField("annotation_ts", annotation.timestamp() + "",
                          Field.Store.YES));
                    }
                    docs.add(doc);
                  }
                  index.put(docs);
                }
              }

              @Override
              public void close() {
                index.close();
              }
            });

    return builder.build();
  }
}
