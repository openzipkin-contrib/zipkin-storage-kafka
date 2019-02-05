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

    public KafkaProducerCall(Producer<String, byte[]> kafkaProducer,
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

    @Override
    public Call<V> clone() {
        return null;
    }
}
