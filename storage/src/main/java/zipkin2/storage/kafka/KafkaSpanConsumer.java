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
package zipkin2.storage.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.Span;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.internal.AggregateCall;
import zipkin2.storage.SpanConsumer;

/**
 * Collected Spans processor.
 *
 * Spans are partitioned by trace ID to enabled downstream processing of spans as part of a trace.
 */
public class KafkaSpanConsumer implements SpanConsumer {
  // Topic names
  final String spansTopicName;
  // Kafka producers
  final Producer<String, byte[]> producer;

  KafkaSpanConsumer(KafkaStorage storage) {
    spansTopicName = storage.spansTopic.name;
    producer = storage.getProducer();
  }

  @Override
  public Call<Void> accept(List<Span> spans) {
    if (spans.isEmpty()) return Call.create(null);
    List<Call<Void>> calls = new ArrayList<>();
    // Collect traceId:spans
    for (Span span : spans) {
      String key = span.traceId();
      byte[] value = SpanBytesEncoder.PROTO3.encode(span);
      calls.add(KafkaProducerCall.create(producer, spansTopicName, key, value));
    }
    return AggregateCall.newVoidCall(calls);
  }

  static class KafkaProducerCall extends Call.Base<Void> {
    static final Logger LOG = LoggerFactory.getLogger(KafkaProducerCall.class);

    final Producer<String, byte[]> kafkaProducer;
    final String topic;
    final String key;
    final byte[] value;

    KafkaProducerCall(
        Producer<String, byte[]> kafkaProducer,
        String topic,
        String key,
        byte[] value) {
      this.kafkaProducer = kafkaProducer;
      this.topic = topic;
      this.key = key;
      this.value = value;
    }

    static Call<Void> create(
        Producer<String, byte[]> producer,
        String topicName,
        String key,
        byte[] value) {
      return new KafkaProducerCall(producer, topicName, key, value);
    }

    @Override
    protected Void doExecute() throws IOException {
      try {
        ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topic, key, value);
        kafkaProducer.send(producerRecord).get();
        return null;
      } catch (Exception e) {
        LOG.error("Error sending span to Kafka", e);
        throw new IOException(e);
      }
    }

    @Override
    protected void doEnqueue(Callback<Void> callback) {
      ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topic, key, value);
      Future<RecordMetadata> ignored = kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
        if (e == null) {
          callback.onSuccess(null);
        } else {
          LOG.error("Error sending span to Kafka", e);
          callback.onError(e);
        }
      });
    }

    @Override public Call<Void> clone() {
      return new KafkaProducerCall(kafkaProducer, topic, key, value);
    }
  }
}
