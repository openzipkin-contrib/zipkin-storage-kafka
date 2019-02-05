package zipkin2.storage.kafka;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.Span;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.kafka.internal.AggregateCall;

import java.util.ArrayList;
import java.util.List;

public class KafkaSpanConsumer implements SpanConsumer {

    final Producer<String, byte[]> producer;

    KafkaSpanConsumer(KafkaStorage kafkaStorage) {
        producer = kafkaStorage.producer;
    }

    @Override
    public Call<Void> accept(List<Span> spans) {
        if (spans.isEmpty()) return Call.create(null);
        List<Call<Void>> calls = new ArrayList<>();
        for (Span span : spans) calls.add(StoreSpanProto3Call.create(producer, span));
        return AggregateCall.create(calls);
    }

    static final class StoreSpanProto3Call extends KafkaProducerCall<Void> implements Call.ErrorHandler<Void> {

        StoreSpanProto3Call(Producer<String, byte[]> kafkaProducer, String topic, String key, byte[] value) {
            super(kafkaProducer, topic, key, value);
        }


        static Call<Void> create(Producer<String, byte[]> producer, Span span) {
            byte[] encodedSpan = SpanBytesEncoder.PROTO3.encode(span);
            StoreSpanProto3Call call = new StoreSpanProto3Call(
                    producer, "zipkin-spans-proto3_v2", span.traceId(), encodedSpan);
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
    }
}
