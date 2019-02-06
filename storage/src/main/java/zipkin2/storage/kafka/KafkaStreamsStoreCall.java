package zipkin2.storage.kafka;

import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import zipkin2.Call;
import zipkin2.Callback;

import java.io.IOException;
import java.util.function.Function;

public abstract class KafkaStreamsStoreCall<K, V, T> extends Call.Base<T> {
    final ReadOnlyKeyValueStore<K, V> store;

    KafkaStreamsStoreCall(ReadOnlyKeyValueStore<K, V> store) {
        this.store = store;
    }

    @Override
    protected T doExecute() throws IOException {
        try {
            return query().apply(store);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void doEnqueue(Callback<T> callback) {
        try {
            callback.onSuccess(query().apply(store));
        } catch (Exception e) {
            callback.onError(e);
        }
    }

    abstract Function<ReadOnlyKeyValueStore<K, V>, T> query();
}
