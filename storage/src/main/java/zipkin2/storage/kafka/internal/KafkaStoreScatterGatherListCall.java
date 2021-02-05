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

import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.StreamsMetadata;

/**
 * Search for all instances containing key/value pairs and aggregate results.
 * <p>
 * Given that we need to collect all values and those might be spread on different instances we do a
 * scatter-gather/parallel call to all instances.
 */
public abstract class KafkaStoreScatterGatherListCall<V> extends KafkaStoreListCall<V> {

  final long limit;

  /**
   * @param kafkaStreams Kafka Streams instance where storeName is located.
   * @param storeName    Store name which will be queried on all instances.
   * @param httpBaseUrl  Base URL composed by protocol, hostname and port.
   * @param httpPath     Http path to query other instances.
   * @param limit        Maximum number of results when collecting results from all instances.
   */
  protected KafkaStoreScatterGatherListCall(
    KafkaStreams kafkaStreams,
    String storeName,
    BiFunction<String, Integer, String> httpBaseUrl,
    String httpPath,
    long limit) {
    super(kafkaStreams, storeName, httpBaseUrl, httpPath);
    this.limit = limit;
  }

  @Override protected CompletableFuture<List<V>> listFuture() {
    List<CompletableFuture<AggregatedHttpResponse>> responseFutures =
      kafkaStreams.allMetadataForStore(storeName)
        .stream()
        .map(StreamsMetadata::hostInfo)
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
          .limit(limit)
          .collect(Collectors.toList()));
  }
}
