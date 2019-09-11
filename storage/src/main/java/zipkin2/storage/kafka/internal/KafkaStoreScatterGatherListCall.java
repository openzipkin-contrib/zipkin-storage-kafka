package zipkin2.storage.kafka.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.linecorp.armeria.client.HttpClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.storage.kafka.KafkaAutocompleteTags;

public abstract class KafkaStoreScatterGatherListCall<V> extends Call.Base<List<V>> {
  static final Logger LOG = LoggerFactory.getLogger(KafkaAutocompleteTags.class);
  static final String HTTP_BASE_URL = "http://%s:%d";
  static final ObjectMapper MAPPER = new ObjectMapper();

  final KafkaStreams kafkaStreams;
  final String storeName;
  final String httpContext;

  protected KafkaStoreScatterGatherListCall(KafkaStreams kafkaStreams, String storeName,
      String httpContext) {
    this.kafkaStreams = kafkaStreams;
    this.storeName = storeName;
    this.httpContext = httpContext;
  }

  /**
   * Search for all instances containing key/value pairs and aggregate results.
   *
   * Given that we need to collect all values and those might be spread on different instances
   * we do a scatter-gather/parallel call to all instances.
   */
  @Override protected List<V> doExecute() throws IOException {
    return kafkaStreams.allMetadataForStore(storeName)
        .parallelStream()
        .map(KafkaStoreScatterGatherListCall::httpClient)
        .map(httpClient -> {
          AggregatedHttpResponse response = httpClient.get(httpContext)
              .aggregate()
              .join();
          if (!response.status().equals(HttpStatus.OK)) return null;
          return response.contentUtf8();
        })
        .map(content -> {
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
        })
        .flatMap(Collection::stream)
        .distinct()
        .collect(Collectors.toList());
  }

  protected abstract V parse(JsonNode node);

  @Override protected void doEnqueue(Callback<List<V>> callback) {
    try {
      callback.onSuccess(doExecute());
    } catch (IOException e) {
      callback.onError(e);
    }
  }

  static HttpClient httpClient(StreamsMetadata metadata) {
    return HttpClient.of(
        String.format(HTTP_BASE_URL, metadata.hostInfo().host(), metadata.hostInfo().port()));
  }
}
