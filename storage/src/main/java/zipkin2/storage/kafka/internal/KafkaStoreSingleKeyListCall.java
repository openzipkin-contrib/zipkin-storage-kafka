/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
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
