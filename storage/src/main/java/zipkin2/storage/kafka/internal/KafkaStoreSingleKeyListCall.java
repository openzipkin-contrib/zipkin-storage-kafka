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
package zipkin2.storage.kafka.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.linecorp.armeria.client.HttpClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.StreamsMetadata;
import zipkin2.Callback;

public abstract class KafkaStoreSingleKeyListCall<V> extends KafkaStoreListCall<V> {
  static final StringSerializer STRING_SERIALIZER = new StringSerializer();

  final String key;

  protected KafkaStoreSingleKeyListCall(KafkaStreams kafkaStreams, String storeName,
      String httpContext, String key) {
    super(kafkaStreams, storeName, httpContext);
    this.key = key;
  }

  /**
   * Search for store by key and get values.
   * <p>
   * Given that key/value pair is based on tag's key and values, we can get the specific instance
   * where values are stored, avoiding scatter-gather/parallel calls.
   */
  @Override protected List<V> doExecute() throws IOException {
    StreamsMetadata metadata = kafkaStreams.metadataForKey(storeName, key, STRING_SERIALIZER);
    HttpClient httpClient = httpClient(metadata);
    AggregatedHttpResponse future = httpClient.get(httpPath).aggregate().join();
    String content = content(future);
    List<V> values = parseList(content);
    return Collections.unmodifiableList(values);
  }

  protected abstract V parse(JsonNode node);

  @Override protected void doEnqueue(Callback<List<V>> callback) {
    try {
      callback.onSuccess(doExecute());
    } catch (IOException e) {
      callback.onError(e);
    }
  }
}
