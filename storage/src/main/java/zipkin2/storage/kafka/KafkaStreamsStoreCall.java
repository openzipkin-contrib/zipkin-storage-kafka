package zipkin2.storage.kafka;

import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import zipkin2.Call;
import zipkin2.Callback;

import java.io.IOException;
import java.util.function.Function;

public abstract class KafkaStreamsStoreCall<V> extends Call.Base<V> {
    final ReadOnlyKeyValueStore<String, byte[]> store;

    KafkaStreamsStoreCall(ReadOnlyKeyValueStore<String, byte[]> store) {
        this.store = store;
    }

    @Override
    protected V doExecute() throws IOException {
        try {
            return query().apply(store);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void doEnqueue(Callback<V> callback) {
        try {
            callback.onSuccess(query().apply(store));
        } catch (Exception e) {
            callback.onError(e);
        }
    }

    abstract Function<ReadOnlyKeyValueStore<String, byte[]>, V> query();
}
