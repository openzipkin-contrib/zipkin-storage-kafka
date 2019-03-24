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

import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import zipkin2.storage.kafka.streams.serdes.SpanNamesSerde;

/**
 * Aggregation of span names per service into set of span names per service.
 */
public class ServiceAggregationStream implements Supplier<Topology> {
  // Kafka topics
  final String spanServiceTopicName;
  final String servicesTopicName;
  // SerDes
  final SpanNamesSerde spanNamesSerde;

  public ServiceAggregationStream(
      String spanServiceTopicName,
      String servicesTopicName) {
    this.spanServiceTopicName = spanServiceTopicName;
    this.servicesTopicName = servicesTopicName;
    spanNamesSerde = new SpanNamesSerde();
  }

  @Override public Topology get() {
    StreamsBuilder builder = new StreamsBuilder();
    // Aggregate ServiceName:SpanName into ServiceName:Set[SpanName]
    builder
        .stream(spanServiceTopicName, Consumed.with(Serdes.String(), Serdes.String()))
        .groupByKey()
        .aggregate(HashSet::new,
            aggregateSpanNames(),
            Materialized
                .<String, Set<String>, KeyValueStore<Bytes, byte[]>>with(Serdes.String(),
                    spanNamesSerde)
                .withCachingEnabled()
                .withLoggingDisabled())
        .toStream()
        .to(servicesTopicName, Produced.with(Serdes.String(), spanNamesSerde));
    return builder.build();
  }

  // Collecting span names into a set of names.
  Aggregator<String, String, Set<String>> aggregateSpanNames() {
    return (serviceName, spanName, spanNames) -> {
      spanNames.add(spanName);
      return spanNames;
    };
  }
}
