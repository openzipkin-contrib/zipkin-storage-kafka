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
package zipkin2.storage.kafka.internal;

import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KafkaStreams;

/**
 * Search for all instances containing key/value pairs and aggregate results.
 * <p>
 * Given that we need to collect all values and those might be spread on different instances we do a
 * scatter-gather/parallel call to all instances.
 */
public abstract class KafkaStoreScatterGatherListCall<V> extends KafkaStoreListCall<V> {

  protected KafkaStoreScatterGatherListCall(
      KafkaStreams kafkaStreams,
      String storeName,
      BiFunction<String, Integer, String> httpBaseUrl,
      String httpPath) {
    super(kafkaStreams, storeName, httpBaseUrl, httpPath);
  }

  protected CompletableFuture<List<V>> listFuture() {
    List<CompletableFuture<AggregatedHttpResponse>> responseFutures =
        kafkaStreams.allMetadataForStore(storeName)
            .stream()
            .map(this::httpClient)
            .map(c -> c.get(httpPath).aggregate()).collect(Collectors.toList());
    return CompletableFuture.allOf(responseFutures.toArray(new CompletableFuture[0]))
        .thenApply(unused ->
            responseFutures.stream()
                .map(s -> s.getNow(AggregatedHttpResponse.of(HttpStatus.INTERNAL_SERVER_ERROR)))
                .map(this::content)
                .map(this::parseList)
                .flatMap(Collection::stream)
                .distinct()
                .collect(Collectors.toList()));
  }
}
