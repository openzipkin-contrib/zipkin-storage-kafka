package zipkin2.storage.kafka.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.linecorp.armeria.client.HttpClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.StreamsMetadata;
import zipkin2.Call;
import zipkin2.Callback;

public abstract class KafkaStoreSingleKeyListCall<V> extends Call.Base<List<V>> {
  static final String HTTP_BASE_URL = "http://%s:%d";
  static final StringSerializer STRING_SERIALIZER = new StringSerializer();
  static final ObjectMapper MAPPER = new ObjectMapper();

  final KafkaStreams kafkaStreams;
  final String storeName;
  final String httpContext;
  final String key;

  protected KafkaStoreSingleKeyListCall(KafkaStreams kafkaStreams, String storeName,
      String httpContext, String key) {
    this.kafkaStreams = kafkaStreams;
    this.storeName = storeName;
    this.httpContext = httpContext;
    this.key = key;
  }

  /**
   * Search for store by key and get values.
   * <p>
   * Given that key/value pair is based on tag's key and values, we can get the specific instance
   * where values are stored, avoiding scatter-gather/parallel calls.
   */
  @Override protected List<V> doExecute() throws IOException {
    StreamsMetadata metadata =
        kafkaStreams.metadataForKey(storeName, key, STRING_SERIALIZER);
    HttpClient httpClient = httpClient(metadata);
    AggregatedHttpResponse response =
        httpClient.get(httpContext).aggregate().join();
    if (!response.status().equals(HttpStatus.OK)) return new ArrayList<>();
    String content = response.contentUtf8();
    ArrayNode arrayNode = (ArrayNode) MAPPER.readTree(content);
    List<V> values = new ArrayList<>();
    for (JsonNode node : arrayNode) {
      V value = parse(node);
      values.add(value);
    }
    return values;
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
