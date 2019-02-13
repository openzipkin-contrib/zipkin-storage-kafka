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
import java.util.function.Supplier;
import zipkin2.Call;
import zipkin2.Callback;

public abstract class KafkaStreamsStoreCall<T> extends Call.Base<T> {

  KafkaStreamsStoreCall() {
  }

  @Override
  protected T doExecute() throws IOException {
    try {
      return query().get();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void doEnqueue(Callback<T> callback) {
    try {
      callback.onSuccess(query().get());
    } catch (Exception e) {
      callback.onError(e);
    }
  }

  abstract Supplier<T> query();
}
