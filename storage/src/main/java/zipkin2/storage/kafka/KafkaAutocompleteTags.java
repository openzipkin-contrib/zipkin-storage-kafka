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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import zipkin2.Call;
import zipkin2.storage.AutocompleteTags;
import zipkin2.storage.kafka.internal.KafkaStreamsStoreCall;

import static zipkin2.storage.kafka.streams.TraceStoreTopologySupplier.AUTOCOMPLETE_TAGS_STORE_NAME;

public class KafkaAutocompleteTags implements AutocompleteTags {
  final KafkaStreams storeStream;

  KafkaAutocompleteTags(KafkaStorage storage) {
    storeStream = storage.getTraceStoreStream();
  }

  @Override public Call<List<String>> getKeys() {
    ReadOnlyKeyValueStore<String, Set<String>> autocompleteTagsStore =
        storeStream.store(AUTOCOMPLETE_TAGS_STORE_NAME,
            QueryableStoreTypes.keyValueStore());
    return new GetKeysCall(autocompleteTagsStore);
  }

  @Override public Call<List<String>> getValues(String key) {
    ReadOnlyKeyValueStore<String, Set<String>> autocompleteTagsStore =
        storeStream.store(AUTOCOMPLETE_TAGS_STORE_NAME,
            QueryableStoreTypes.keyValueStore());
    return new GetValuesCall(autocompleteTagsStore, key);
  }

  static class GetKeysCall extends KafkaStreamsStoreCall<List<String>> {
    final ReadOnlyKeyValueStore<String, Set<String>> autocompleteTagsStore;

    GetKeysCall(ReadOnlyKeyValueStore<String, Set<String>> autocompleteTagsStore) {
      this.autocompleteTagsStore = autocompleteTagsStore;
    }

    @Override protected List<String> query() {
      List<String> keys = new ArrayList<>();
      autocompleteTagsStore.all().forEachRemaining(keyValue -> keys.add(keyValue.key));
      return keys;
    }

    @Override public Call<List<String>> clone() {
      return new GetKeysCall(autocompleteTagsStore);
    }
  }

  static class GetValuesCall extends KafkaStreamsStoreCall<List<String>> {
    final ReadOnlyKeyValueStore<String, Set<String>> autocompleteTagsStore;
    final String key;

    GetValuesCall(
        ReadOnlyKeyValueStore<String, Set<String>> autocompleteTagsStore, String key) {
      this.autocompleteTagsStore = autocompleteTagsStore;
      this.key = key;
    }

    @Override protected List<String> query() {
      Set<String> valuesSet = autocompleteTagsStore.get(key);
      if (valuesSet == null) return new ArrayList<>();
      return new ArrayList<>(valuesSet);
    }

    @Override public Call<List<String>> clone() {
      return new GetValuesCall(autocompleteTagsStore, key);
    }
  }
}
