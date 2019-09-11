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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KafkaStreams;
import zipkin2.Callback;

public abstract class KafkaStoreScatterGatherListCall<V> extends KafkaStoreListCall<V> {
  final KafkaStreams kafkaStreams;
  final String storeName;
  final String httpPath;

  protected KafkaStoreScatterGatherListCall(
      KafkaStreams kafkaStreams,
      String storeName,
      String httpPath) {
    super(kafkaStreams, storeName, httpPath);
    this.kafkaStreams = kafkaStreams;
    this.storeName = storeName;
    this.httpPath = httpPath;
  }

  /**
   * Search for all instances containing key/value pairs and aggregate results.
   * <p>
   * Given that we need to collect all values and those might be spread on different instances we do
   * a scatter-gather/parallel call to all instances.
   */
  @Override protected List<V> doExecute() throws IOException {
    return kafkaStreams.allMetadataForStore(storeName)
        .parallelStream()
        .map(this::httpClient)
        .map(this::callHttpPath)
        .map(this::parseList)
        .flatMap(Collection::stream)
        .distinct()
        .collect(Collectors.toList());
  }

  @Override protected void doEnqueue(Callback<List<V>> callback) {
    try {
      callback.onSuccess(doExecute());
    } catch (IOException e) {
      callback.onError(e);
    }
  }
}
