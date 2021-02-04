/*
 * Copyright 2019-2021 The OpenZipkin Authors
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
package zipkin2.storage.kafka.internal;

import com.linecorp.armeria.client.WebClient;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;

/**
 * Search for store by key and get values.
 * <p>
 * Given that key/value pair is based on tag's key and values, we can get the specific instance
 * where values are stored, avoiding scatter-gather/parallel calls.
 */
public abstract class KafkaStoreSingleKeyListCall<V> extends KafkaStoreListCall<V> {
  static final StringSerializer STRING_SERIALIZER = new StringSerializer();

  final String key;

  protected KafkaStoreSingleKeyListCall(
    KafkaStreams kafkaStreams,
    String storeName,
    BiFunction<String, Integer, String> httpBaseUrl,
    String httpPath,
    String key) {
    super(kafkaStreams, storeName, httpBaseUrl, httpPath);
    this.key = key;
  }

  @Override protected CompletableFuture<List<V>> listFuture() {
    KeyQueryMetadata metadata = kafkaStreams.queryMetadataForKey(storeName, key, STRING_SERIALIZER);
    WebClient httpClient = httpClient(metadata.activeHost());
    return httpClient.get(httpPath)
      .aggregate()
      .thenApply(response -> {
        String content = content(response);
        return parseList(content);
      });
  }
}
