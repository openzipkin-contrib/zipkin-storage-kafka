/*
 * Copyright 2019 The OpenZipkin Authors
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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.function.BiFunction;
import org.apache.kafka.streams.KafkaStreams;
import zipkin2.Call;
import zipkin2.storage.AutocompleteTags;
import zipkin2.storage.kafka.internal.KafkaStoreScatterGatherListCall;
import zipkin2.storage.kafka.internal.KafkaStoreSingleKeyListCall;
import zipkin2.storage.kafka.streams.TraceStoreTopologySupplier;

import static zipkin2.storage.kafka.streams.TraceStoreTopologySupplier.AUTOCOMPLETE_TAGS_STORE_NAME;

/**
 * Autocomplete tags query component based on Kafka Streams local store built by {@link
 * TraceStoreTopologySupplier}
 * <p>
 * These stores are currently supporting only single instance as there is not mechanism implemented
 * for scatter gather data from different instances.
 */
final class KafkaAutocompleteTags implements AutocompleteTags {
  static final long AUTOCOMPLETE_TAGS_LIMIT = 1_000;

  final KafkaStorage storage;
  final BiFunction<String, Integer, String> httpBaseUrl;
  final boolean traceSearchEnabled;

  KafkaAutocompleteTags(KafkaStorage storage) {
    this.storage = storage;
    this.traceSearchEnabled = storage.traceSearchEnabled;
    httpBaseUrl = storage.httpBaseUrl;
  }

  @Override public Call<List<String>> getKeys() {
    if (traceSearchEnabled) {
      return new GetTagKeysCall(storage.getTraceStoreStream(), httpBaseUrl);
    } else {
      return Call.emptyList();
    }
  }

  @Override public Call<List<String>> getValues(String key) {
    if (traceSearchEnabled) {
      return new GetTagValuesCall(storage.getTraceStoreStream(), httpBaseUrl, key);
    } else {
      return Call.emptyList();
    }
  }

  static class GetTagKeysCall extends KafkaStoreScatterGatherListCall<String> {
    final KafkaStreams traceStoreStream;
    final BiFunction<String, Integer, String> httpBaseUrl;

    GetTagKeysCall(KafkaStreams traceStoreStream,
        BiFunction<String, Integer, String> httpBaseUrl) {
      super(
          traceStoreStream,
          AUTOCOMPLETE_TAGS_STORE_NAME,
          httpBaseUrl,
          "/autocompleteTags",
          AUTOCOMPLETE_TAGS_LIMIT);
      this.traceStoreStream = traceStoreStream;
      this.httpBaseUrl = httpBaseUrl;
    }

    @Override protected String parseItem(JsonNode node) {
      return node.textValue();
    }

    @Override public Call<List<String>> clone() {
      return new GetTagKeysCall(traceStoreStream, httpBaseUrl);
    }
  }

  static class GetTagValuesCall extends KafkaStoreSingleKeyListCall<String> {
    final KafkaStreams traceStoreStream;
    final BiFunction<String, Integer, String> httpBaseUrl;
    final String tagKey;

    GetTagValuesCall(KafkaStreams traceStoreStream,
        BiFunction<String, Integer, String> httpBaseUrl,
        String tagKey) {
      super(
          traceStoreStream,
          AUTOCOMPLETE_TAGS_STORE_NAME,
          httpBaseUrl,
          "/autocompleteTags/" + tagKey,
          tagKey);
      this.traceStoreStream = traceStoreStream;
      this.httpBaseUrl = httpBaseUrl;
      this.tagKey = tagKey;
    }

    @Override public Call<List<String>> clone() {
      return new GetTagValuesCall(traceStoreStream, httpBaseUrl, tagKey);
    }

    @Override protected String parseItem(JsonNode node) {
      return node.textValue();
    }
  }
}
