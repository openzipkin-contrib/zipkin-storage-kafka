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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.linecorp.armeria.client.HttpClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zipkin2.Call;
import zipkin2.Callback;

public abstract class KafkaStoreListCall<V> extends Call.Base<List<V>> {
  static final Logger LOG = LogManager.getLogger();
  static final ObjectMapper MAPPER = new ObjectMapper();

  final KafkaStreams kafkaStreams;
  final String storeName;
  final BiFunction<String, Integer, String> httpBaseUrl;
  final String httpPath;

  protected KafkaStoreListCall(
    KafkaStreams kafkaStreams,
    String storeName,
    BiFunction<String, Integer, String> httpBaseUrl,
    String httpPath) {
    this.kafkaStreams = kafkaStreams;
    this.storeName = storeName;
    this.httpBaseUrl = httpBaseUrl;
    this.httpPath = httpPath;
  }

  @SuppressWarnings("MixedMutabilityReturnType")
  protected List<V> parseList(String content) {
    try {
      if (content == null) return Collections.emptyList();
      ArrayNode arrayNode = (ArrayNode) MAPPER.readTree(content);
      List<V> values = new ArrayList<>();
      for (JsonNode node : arrayNode) {
        V value = parseItem(node);
        values.add(value);
      }
      return values;
    } catch (IOException e) {
      LOG.debug("Error reading json response", e);
      return Collections.emptyList();
    }
  }

  protected String content(AggregatedHttpResponse response) {
    if (!response.status().equals(HttpStatus.OK)) return null;
    return response.contentUtf8();
  }

  protected HttpClient httpClient(HostInfo hostInfo) {
    return HttpClient.of(httpBaseUrl.apply(hostInfo.host(), hostInfo.port()));
  }

  @Override protected List<V> doExecute() {
    return listFuture().join();
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  @Override protected void doEnqueue(Callback<List<V>> callback) {
    listFuture().handle((response, t) -> {
      if (t != null) {
        callback.onError(t);
      } else {
        try {
          callback.onSuccess(response);
        } catch (Throwable t1) {
          propagateIfFatal(t1);
          callback.onError(t1);
        }
      }
      return null;
    });
  }

  /**
   * Calling http service to obtain {@code list of V} items
   */
  protected abstract CompletableFuture<List<V>> listFuture();

  /**
   * Parse list element from json into {@code type V}
   *
   * @see #parseList(String)
   */
  protected abstract V parseItem(JsonNode node) throws JsonProcessingException;
}
