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
import org.apache.kafka.streams.state.StreamsMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.storage.kafka.KafkaAutocompleteTags;

public abstract class KafkaStoreListCall<V> extends Call.Base<List<V>> {
  static final Logger LOG = LoggerFactory.getLogger(KafkaAutocompleteTags.class);
  static final ObjectMapper MAPPER = new ObjectMapper();

  final KafkaStreams kafkaStreams;
  final String storeName;
  final BiFunction<String, Integer, String> httpBaseUrl;
  final String httpPath;

  KafkaStoreListCall(
      KafkaStreams kafkaStreams,
      String storeName,
      BiFunction<String, Integer, String> httpBaseUrl,
      String httpPath) {
    this.kafkaStreams = kafkaStreams;
    this.storeName = storeName;
    this.httpBaseUrl = httpBaseUrl;
    this.httpPath = httpPath;
  }

  List<V> parseList(String content) {
    try {
      ArrayNode arrayNode = (ArrayNode) MAPPER.readTree(content);
      List<V> values = new ArrayList<>();
      for (JsonNode node : arrayNode) {
        V value = parse(node);
        values.add(value);
      }
      return Collections.unmodifiableList(values);
    } catch (IOException e) {
      LOG.error("Error reading json response", e);
      return Collections.emptyList();
    }
  }

  String content(AggregatedHttpResponse response) {
    if (!response.status().equals(HttpStatus.OK)) return null;
    return response.contentUtf8();
  }

  HttpClient httpClient(StreamsMetadata metadata) {
    return HttpClient.of(httpBaseUrl.apply(metadata.hostInfo().host(), metadata.hostInfo().port()));
  }

  @Override protected List<V> doExecute() throws IOException {
    return listFuture().join();
  }

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

  protected abstract V parse(JsonNode node);

  protected abstract CompletableFuture<List<V>> listFuture();
}
