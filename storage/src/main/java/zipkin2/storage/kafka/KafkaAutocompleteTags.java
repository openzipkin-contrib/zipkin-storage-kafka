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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.HttpClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.storage.AutocompleteTags;
import zipkin2.storage.kafka.streams.TraceStoreTopologySupplier;

import static zipkin2.storage.kafka.streams.TraceStoreTopologySupplier.AUTOCOMPLETE_TAGS_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStoreTopologySupplier.SPAN_NAMES_STORE_NAME;

/**
 * Autocomplete tags query component based on Kafka Streams local store built by {@link
 * TraceStoreTopologySupplier}
 *
 * These stores are currently supporting only single instance as there is not mechanism implemented
 * for scatter gather data from different instances.
 */
public class KafkaAutocompleteTags implements AutocompleteTags {
  static final Logger LOG = LoggerFactory.getLogger(KafkaAutocompleteTags.class);
  static final ObjectMapper MAPPER = new ObjectMapper();
  static final String HTTP_BASE_URL = "http://%s:%d";

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

  static class GetTagKeysCall extends Call.Base<List<String>> {

    final KafkaStreams traceStoreStream;

    GetTagKeysCall(KafkaStreams traceStoreStream) {
      this.traceStoreStream = traceStoreStream;
    }

    @Override protected List<String> doExecute() throws IOException {
      return traceStoreStream.allMetadataForStore(AUTOCOMPLETE_TAGS_STORE_NAME)
          .parallelStream()
          .map(KafkaAutocompleteTags::httpClient)
          .map(httpClient -> {
            AggregatedHttpResponse response = httpClient.get("/autocompleteTags")
                .aggregate()
                .join();
            if (!HttpStatus.OK.equals(response.status())) return null;
            return response.contentUtf8();
          })
          .map(content -> {
            try {
              String[] values = MAPPER.readValue(content, String[].class);
              return Arrays.asList(values);
            } catch (IOException e) {
              LOG.error("Error reading json response", e);
              return Collections.<String>emptyList();
            }
          })
          .flatMap(Collection::stream)
          .distinct()
          .collect(Collectors.toList());
    }

    @Override protected void doEnqueue(Callback<List<String>> callback) {
      try {
        callback.onSuccess(doExecute());
      } catch (IOException e) {
        callback.onError(e);
      }
    }

    @Override public Call<List<String>> clone() {
      return new GetTagKeysCall(traceStoreStream);
    }
  }

  static class GetTagValuesCall extends Call.Base<List<String>> {

    final KafkaStreams traceStoreStream;
    final String tagKey;

    GetTagValuesCall(KafkaStreams traceStoreStream, String tagKey) {
      this.traceStoreStream = traceStoreStream;
      this.tagKey = tagKey;
    }

    @Override protected List<String> doExecute() throws IOException {
      StreamsMetadata metadata =
          traceStoreStream.metadataForKey(SPAN_NAMES_STORE_NAME, tagKey,
              new StringSerializer());
      HttpClient httpClient = httpClient(metadata);
      AggregatedHttpResponse response =
          httpClient.get(String.format("/autocompleteTags/%s", tagKey))
              .aggregate()
              .join();
      if (!HttpStatus.OK.equals(response.status())) return new ArrayList<>();
      String content = response.contentUtf8();
      try {
        String[] values = MAPPER.readValue(content, String[].class);
        return Arrays.asList(values);
      } catch (IOException e) {
        LOG.error("Error reading json response", e);
        return Collections.emptyList();
      }
    }

    @Override protected void doEnqueue(Callback<List<String>> callback) {
      try {
        callback.onSuccess(doExecute());
      } catch (IOException e) {
        callback.onError(e);
      }
    }

    @Override public Call<List<String>> clone() {
      return new GetTagValuesCall(traceStoreStream, tagKey);
    }
  }

  static HttpClient httpClient(StreamsMetadata metadata) {
    return HttpClient.of(
        String.format(HTTP_BASE_URL, metadata.hostInfo().host(), metadata.hostInfo().port()));
  }
}
