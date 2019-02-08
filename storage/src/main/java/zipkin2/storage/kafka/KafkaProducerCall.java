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

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import zipkin2.Call;
import zipkin2.Callback;

import java.io.IOException;

public abstract class KafkaProducerCall<V> extends Call.Base<V> {
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
                callback.onError(e);
            }
        });
    }
}
