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
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.Call;
import zipkin2.storage.kafka.KafkaAutocompleteTags;

public abstract class KafkaStoreListCall<V> extends Call.Base<List<V>> {
  static final Logger LOG = LoggerFactory.getLogger(KafkaAutocompleteTags.class);
  static final String HTTP_BASE_URL = "http://%s:%d";
  static final ObjectMapper MAPPER = new ObjectMapper();

  final KafkaStreams kafkaStreams;
  final String storeName;
  final String httpPath;

  KafkaStoreListCall(KafkaStreams kafkaStreams, String storeName, String httpPath) {
    this.kafkaStreams = kafkaStreams;
    this.storeName = storeName;
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
      return Collections.<V>emptyList();
    }
  }

  protected abstract V parse(JsonNode node);

  String content(AggregatedHttpResponse response) {
    if (!response.status().equals(HttpStatus.OK)) return null;
    return response.contentUtf8();
  }

  HttpClient httpClient(StreamsMetadata metadata) {
    return HttpClient.of(
        String.format(HTTP_BASE_URL, metadata.hostInfo().host(), metadata.hostInfo().port()));
  }
}
