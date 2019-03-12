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
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.Span;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.kafka.internal.AggregateCall;

/**
 * Processing of Spans consumed.
 *
 * Supported by a Kafka Producer, spans are stored in a Kafka Topic.
 */
public class KafkaSpanConsumer implements SpanConsumer {

  final String spansTopic;
  final Producer<String, byte[]> kafkaProducer;

  KafkaSpanConsumer(KafkaStorage storage) {
    spansTopic = storage.spansTopic.name;
    kafkaProducer = storage.producer;
  }

  @Override
  public Call<Void> accept(List<Span> spans) {
    if (spans.isEmpty()) return Call.create(null);
    List<Call<Void>> calls = new ArrayList<>();
    for (Span span : spans) calls.add(StoreSpanCall.create(kafkaProducer, spansTopic, span));
    return AggregateCall.create(calls);
  }

  static final class StoreSpanCall extends KafkaProducerCall<Void>
      implements Call.ErrorHandler<Void> {

    StoreSpanCall(Producer<String, byte[]> kafkaProducer, String topic, String key, byte[] value) {
      super(kafkaProducer, topic, key, value);
    }

    static Call<Void> create(Producer<String, byte[]> producer, String spansTopic, Span span) {
      byte[] encodedSpan = SpanBytesEncoder.PROTO3.encode(span);
      StoreSpanCall call = new StoreSpanCall(producer, spansTopic, span.id(), encodedSpan);
      return call.handleError(call);
    }

    @Override
    public void onErrorReturn(Throwable error, Callback<Void> callback) {
      callback.onError(error);
    }

    @Override
    Void convert(RecordMetadata recordMetadata) {
      return null;
    }

    @Override
    public Call<Void> clone() {
      return new StoreSpanCall(kafkaProducer, topic, key, value);
    }
  }

  abstract static class KafkaProducerCall<V> extends Call.Base<V> {
    static final Logger LOG = LoggerFactory.getLogger(KafkaProducerCall.class);

    final Producer<String, byte[]> kafkaProducer;
    final String topic;
    final String key;
    final byte[] value;

    KafkaProducerCall(Producer<String, byte[]> kafkaProducer,
        String topic,
        String key,
        byte[] value) {
      this.kafkaProducer = kafkaProducer;
      this.topic = topic;
      this.key = key;
      this.value = value;
    }

    @Override
    protected V doExecute() throws IOException {
      try {
        ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topic, key, value);
        RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
        return convert(recordMetadata);
      } catch (Exception e) {
        LOG.error("Error sending span to Kafka", e);
        throw new IOException(e);
      }
    }

    abstract V convert(RecordMetadata recordMetadata);

    @Override
    @SuppressWarnings("FutureReturnValueIgnored")
    protected void doEnqueue(Callback<V> callback) {
      ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topic, key, value);
      kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
        if (e == null) {
          callback.onSuccess(convert(recordMetadata));
        } else {
          LOG.error("Error sending span to Kafka", e);
          callback.onError(e);
        }
      });
    }
  }
}
