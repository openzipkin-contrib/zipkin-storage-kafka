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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
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
public class KafkaAutocompleteTags implements AutocompleteTags {
  final KafkaStreams traceStoreStream;

  KafkaAutocompleteTags(KafkaStorage storage) {
    traceStoreStream = storage.getTraceStoreStream();
  }

  @Override public Call<List<String>> getKeys() {
    return new GetTagKeysCall(traceStoreStream);
  }

  @Override public Call<List<String>> getValues(String key) {
    return new GetTagValuesCall(traceStoreStream, key);
  }

  static class GetTagKeysCall extends KafkaStoreScatterGatherListCall<String> {

    final KafkaStreams traceStoreStream;

    GetTagKeysCall(KafkaStreams traceStoreStream) {
      super(traceStoreStream, AUTOCOMPLETE_TAGS_STORE_NAME, "/autocompleteTags");
      this.traceStoreStream = traceStoreStream;
    }

    @Override protected String parse(JsonNode node) {
      return node.textValue();
    }

    @Override public Call<List<String>> clone() {
      return new GetTagKeysCall(traceStoreStream);
    }
  }

  static class GetTagValuesCall extends KafkaStoreSingleKeyListCall<String> {
    final KafkaStreams traceStoreStream;
    final String tagKey;

    GetTagValuesCall(KafkaStreams traceStoreStream, String tagKey) {
      super(traceStoreStream, AUTOCOMPLETE_TAGS_STORE_NAME,
          String.format("/autocompleteTags/%s", tagKey), tagKey);
      this.traceStoreStream = traceStoreStream;
      this.tagKey = tagKey;
    }

    @Override public Call<List<String>> clone() {
      return new GetTagValuesCall(traceStoreStream, tagKey);
    }

    @Override protected String parse(JsonNode node) {
      return node.textValue();
    }
  }
}
