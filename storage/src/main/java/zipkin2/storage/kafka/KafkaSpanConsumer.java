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
import java.util.Collections;
import java.util.List;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.Span;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.internal.AggregateCall;
import zipkin2.reporter.AwaitableCallback;
import zipkin2.reporter.kafka.KafkaSender;
import zipkin2.storage.SpanConsumer;

/**
 * Span Consumer to compensate current {@link KafkaSender} distribution of span batched without key.
 *
 * This component split batch into individual spans keyed by trace ID to enabled downstream
 * processing of spans as part of a trace.
 */
public class KafkaSpanConsumer implements SpanConsumer {
  // Topic names
  final String spansTopicName;
  // Kafka producers
  final Producer<String, byte[]> producer;

  KafkaSpanConsumer(KafkaStorage storage) {
    spansTopicName = storage.spansTopicName;
    producer = storage.getProducer();
  }

  @Override
  public Call<Void> accept(List<Span> spans) {
    if (spans.isEmpty()) return Call.create(null);
    List<Call<Void>> calls = new ArrayList<>();
    // Collect traceId:spans
    for (Span span : spans) {
      String key = span.traceId();
      byte[] value = SpanBytesEncoder.PROTO3.encodeList(Collections.singletonList(span));
      calls.add(KafkaProducerCall.create(producer, spansTopicName, key, value));
    }
    return AggregateCall.newVoidCall(calls);
  }

  static class KafkaProducerCall extends Call.Base<Void> {
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
        String topic,
        String key,
        byte[] value) {
      return new KafkaProducerCall(producer, topic, key, value);
    }

    @Override
    @SuppressWarnings("FutureReturnValueIgnored")
    protected Void doExecute() throws IOException {
      AwaitableCallback callback = new AwaitableCallback();
      kafkaProducer.send(new ProducerRecord<>(topic, key, value), new CallbackAdapter(callback));
      callback.await();
      return null;
    }

    @Override
    @SuppressWarnings("FutureReturnValueIgnored")
    protected void doEnqueue(Callback<Void> callback) {
      kafkaProducer.send(new ProducerRecord<>(topic, key, value), new CallbackAdapter(callback));
    }

    @Override public Call<Void> clone() {
      return new KafkaProducerCall(kafkaProducer, topic, key, value);
    }

    static final class CallbackAdapter implements org.apache.kafka.clients.producer.Callback {
      final Callback<Void> delegate;

      CallbackAdapter(Callback<Void> delegate) {
        this.delegate = delegate;
      }

      @Override public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
          delegate.onSuccess(null);
        } else {
          delegate.onError(exception);
        }
      }

      @Override public String toString() {
        return delegate.toString();
      }
    }
  }
}
