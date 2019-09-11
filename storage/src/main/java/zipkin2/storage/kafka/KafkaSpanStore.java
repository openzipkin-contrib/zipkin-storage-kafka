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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.linecorp.armeria.client.HttpClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.RequestHeaders;
import java.io.IOException;
import java.nio.ByteBuffer;
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
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.codec.DependencyLinkBytesDecoder;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.ServiceAndSpanNames;
import zipkin2.storage.SpanStore;
import zipkin2.storage.kafka.internal.Traces;
import zipkin2.storage.kafka.streams.DependencyStoreTopologySupplier;
import zipkin2.storage.kafka.streams.TraceStoreTopologySupplier;

import static zipkin2.storage.kafka.streams.DependencyStoreTopologySupplier.DEPENDENCIES_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStoreTopologySupplier.REMOTE_SERVICE_NAMES_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStoreTopologySupplier.SERVICE_NAMES_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStoreTopologySupplier.SPAN_NAMES_STORE_NAME;
import static zipkin2.storage.kafka.streams.TraceStoreTopologySupplier.TRACES_STORE_NAME;

/**
 * Span store backed by Kafka Stream distributed state stores built by {@link
 * TraceStoreTopologySupplier} and {@link DependencyStoreTopologySupplier}, and made accessible by
 * {@link  KafkaStoreHttpService}.
 */
public class KafkaSpanStore implements SpanStore, ServiceAndSpanNames {
  static final Logger LOG = LoggerFactory.getLogger(KafkaAutocompleteTags.class);
  static final ObjectMapper MAPPER = new ObjectMapper();
  static final String HTTP_BASE_URL = "http://%s:%d";
  // Kafka Streams Store provider
  final KafkaStreams traceStoreStream;
  final KafkaStreams dependencyStoreStream;

  KafkaSpanStore(KafkaStorage storage) {
    traceStoreStream = storage.getTraceStoreStream();
    dependencyStoreStream = storage.getDependencyStoreStream();
  }

  @Override public Call<List<List<Span>>> getTraces(QueryRequest request) {
    return new GetTracesCall(traceStoreStream, request);
  }

  @Override
  public Call<List<Span>> getTrace(String traceId) {
    return new GetTraceCall(traceStoreStream, traceId);
  }

  @Deprecated @Override public Call<List<String>> getServiceNames() {
    return new GetServiceNamesCall(traceStoreStream);
  }

  @Deprecated @Override public Call<List<String>> getSpanNames(String serviceName) {
    return new GetSpanNamesCall(traceStoreStream, serviceName);
  }

  @Override public Call<List<String>> getRemoteServiceNames(String serviceName) {
    return new GetRemoteServiceNamesCall(traceStoreStream, serviceName);
  }

  @Override public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
    return new GetDependenciesCall(dependencyStoreStream, endTs, lookback);
  }

  static class GetServiceNamesCall extends Call.Base<List<String>> {
    final KafkaStreams traceStoreStream;

    GetServiceNamesCall(KafkaStreams traceStoreStream) {
      this.traceStoreStream = traceStoreStream;
    }

    @Override protected List<String> doExecute() throws IOException {
      return traceStoreStream.allMetadataForStore(SERVICE_NAMES_STORE_NAME)
          .parallelStream()
          .map(KafkaSpanStore::httpClient)
          .map(httpClient -> {
            AggregatedHttpResponse response = httpClient.get("/serviceNames")
                .aggregate()
                .join();
            if (!response.status().equals(HttpStatus.OK)) return null;
            return response.contentUtf8();
          })
          .map(content -> {
            if (content == null) return new ArrayList<String>();
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
      return new GetServiceNamesCall(traceStoreStream);
    }
  }

  static class GetSpanNamesCall extends Call.Base<List<String>> {
    final KafkaStreams traceStoreStream;
    final String serviceName;

    GetSpanNamesCall(KafkaStreams traceStoreStream, String serviceName) {
      this.traceStoreStream = traceStoreStream;
      this.serviceName = serviceName;
    }

    @Override protected List<String> doExecute() throws IOException {
      StreamsMetadata metadata =
          traceStoreStream.metadataForKey(SPAN_NAMES_STORE_NAME, serviceName,
              new StringSerializer());
      HttpClient httpClient = httpClient(metadata);
      AggregatedHttpResponse response =
          httpClient.get(String.format("/serviceNames/%s/spanNames", serviceName))
              .aggregate()
              .join();
      if (!response.status().equals(HttpStatus.OK)) return new ArrayList<>();
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
      return new GetSpanNamesCall(traceStoreStream, serviceName);
    }
  }

  static class GetRemoteServiceNamesCall extends Call.Base<List<String>> {
    final KafkaStreams traceStoreStream;
    final String serviceName;

    GetRemoteServiceNamesCall(KafkaStreams traceStoreStream, String serviceName) {
      this.traceStoreStream = traceStoreStream;
      this.serviceName = serviceName;
    }

    @Override protected List<String> doExecute() throws IOException {
      StreamsMetadata metadata =
          traceStoreStream.metadataForKey(REMOTE_SERVICE_NAMES_STORE_NAME, serviceName,
              new StringSerializer());
      HttpClient httpClient = httpClient(metadata);
      AggregatedHttpResponse response =
          httpClient.get(String.format("/serviceNames/%s/remoteServiceNames", serviceName))
              .aggregate()
              .join();
      if (!response.status().equals(HttpStatus.OK)) return new ArrayList<>();
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
      return new GetRemoteServiceNamesCall(traceStoreStream, serviceName);
    }
  }

  static class GetTracesCall extends Call.Base<List<List<Span>>> {
    final KafkaStreams traceStoreStream;
    final QueryRequest request;

    GetTracesCall(KafkaStreams traceStoreStream, QueryRequest request) {
      this.traceStoreStream = traceStoreStream;
      this.request = request;
    }

    @Override protected List<List<Span>> doExecute() throws IOException {
      List<List<Span>> traces = traceStoreStream.allMetadataForStore(TRACES_STORE_NAME)
          .parallelStream()
          .map(KafkaSpanStore::httpClient)
          .map(httpClient -> {
            String path = String.format("/traces?%s%s%s%s%s%s%s%s%s",
                request.serviceName() == null ? "" : "serviceName=" + request.serviceName() + "&",
                request.remoteServiceName() == null ? ""
                    : "remoteServiceName=" + request.remoteServiceName() + "&",
                request.spanName() == null ? "" : "spanName=" + request.spanName() + "&",
                request.annotationQueryString() == null ? ""
                    : "annotationQuery=" + request.annotationQueryString() + "&",
                request.minDuration() == null ? "" : "minDuration=" + request.minDuration() + "&",
                request.maxDuration() == null ? "" : "maxDuration=" + request.maxDuration() + "&",
                "endTs=" + request.endTs() + "&",
                "lookback=" + request.lookback() + "&",
                "limit=" + request.limit());
            AggregatedHttpResponse response =
                httpClient.execute(RequestHeaders.of(HttpMethod.GET, path)).aggregate().join();
            if (!response.status().equals(HttpStatus.OK)) {
              LOG.error("Error querying traces {}", response.contentUtf8());
              return null;
            }
            return response.contentUtf8();
          })
          .map(response -> {
            if (response == null) return new ArrayList<List<Span>>();
            try {
              ArrayNode array = (ArrayNode) MAPPER.readTree(response);
              List<List<Span>> result = new ArrayList<>();
              for (JsonNode node : array) {
                result.add(SpanBytesDecoder.JSON_V2.decodeList(node.toString().getBytes()));
              }
              return result;
            } catch (IOException e) {
              LOG.error("Error parsing response from get traces", e);
              return new ArrayList<List<Span>>();
            }
          })
          .flatMap(Collection::stream)
          .distinct()
          .collect(Collectors.toList());
      return traces.subList(0, Math.min(request.limit(), traces.size()));
    }

    @Override protected void doEnqueue(Callback<List<List<Span>>> callback) {
      try {
        callback.onSuccess(doExecute());
      } catch (IOException e) {
        callback.onError(e);
      }
    }

    @Override public Call<List<List<Span>>> clone() {
      return new GetTracesCall(traceStoreStream, request);
    }
  }

  static class GetTraceCall extends Call.Base<List<Span>> {
    final KafkaStreams traceStoreStream;
    final String traceId;

    GetTraceCall(KafkaStreams traceStoreStream, String traceId) {
      this.traceStoreStream = traceStoreStream;
      this.traceId = traceId;
    }

    @Override protected List<Span> doExecute() throws IOException {
      StreamsMetadata metadata =
          traceStoreStream.metadataForKey(TRACES_STORE_NAME, traceId, new StringSerializer());
      HttpClient httpClient = httpClient(metadata);
      AggregatedHttpResponse response = httpClient.get(String.format("/traces/%s", traceId))
          .aggregate()
          .join();
      if (!response.status().equals(HttpStatus.OK)) return new ArrayList<>();
      HttpData content = response.content();
      return SpanBytesDecoder.JSON_V2.decodeList(ByteBuffer.wrap(content.array()));
    }

    @Override protected void doEnqueue(Callback<List<Span>> callback) {
      try {
        callback.onSuccess(doExecute());
      } catch (IOException e) {
        callback.onError(e);
      }
    }

    @Override public Call<List<Span>> clone() {
      return new GetTraceCall(traceStoreStream, traceId);
    }
  }

  static class GetDependenciesCall extends Call.Base<List<DependencyLink>> {
    final KafkaStreams dependencyStoreStream;
    final long endTs, lookback;

    GetDependenciesCall(KafkaStreams dependencyStoreStream,
        long endTs, long lookback) {
      this.dependencyStoreStream = dependencyStoreStream;
      this.endTs = endTs;
      this.lookback = lookback;
    }

    @Override protected List<DependencyLink> doExecute() throws IOException {
      return dependencyStoreStream.allMetadataForStore(DEPENDENCIES_STORE_NAME)
          .parallelStream()
          .map(KafkaSpanStore::httpClient)
          .map(httpClient -> {
            AggregatedHttpResponse response = httpClient.get(
                String.format("/dependencies?endTs=%s&lookback=%s", endTs, lookback))
                .aggregate()
                .join();
            if (!response.status().equals(HttpStatus.OK)) return null;
            return response.content();
          })
          .map(content -> {
            if (content == null) return new ArrayList<DependencyLink>();
            return DependencyLinkBytesDecoder.JSON_V1.decodeList(content.array());
          })
          .flatMap(Collection::stream)
          .distinct()
          .collect(Collectors.toList());
    }

    @Override protected void doEnqueue(Callback<List<DependencyLink>> callback) {
      try {
        callback.onSuccess(doExecute());
      } catch (IOException e) {
        callback.onError(e);
      }
    }

    @Override public Call<List<DependencyLink>> clone() {
      return new GetDependenciesCall(dependencyStoreStream, endTs, lookback);
    }
  }

  static HttpClient httpClient(StreamsMetadata metadata) {
    return HttpClient.of(
        String.format(HTTP_BASE_URL, metadata.hostInfo().host(), metadata.hostInfo().port()));
  }
}
