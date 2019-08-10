package zipkin2.storage.kafka.internal;

import java.io.IOException;
import zipkin2.Call;
import zipkin2.Callback;

public abstract class KafkaStreamsStoreCall<T> extends Call.Base<T> {

  protected KafkaStreamsStoreCall() {
  }

  @Override protected T doExecute() throws IOException {
    try {
      return query();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override protected void doEnqueue(Callback<T> callback) {
    try {
      callback.onSuccess(query());
    } catch (Exception e) {
      callback.onError(e);
    }
  }

  protected abstract T query();
}
