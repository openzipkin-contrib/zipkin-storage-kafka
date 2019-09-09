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

import com.google.gson.Gson;
import com.linecorp.armeria.client.HttpClient;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.RequestHeaders;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.StreamsMetadata;
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
 * Span store backed by Kafka Stream local stores built by {@link TraceStoreTopologySupplier} and
 * {@link DependencyStoreTopologySupplier}.
 * <p>
 * These stores are currently supporting only single instance as there is not mechanism implemented
 * for scatter gather data from different instances.
 */
public class KafkaSpanStore implements SpanStore, ServiceAndSpanNames {
  // Kafka Streams Store provider
  final KafkaStreams traceStoreStream;
  final KafkaStreams dependencyStoreStream;

  KafkaSpanStore(KafkaStorage storage) {
    traceStoreStream = storage.getTraceStoreStream();
    dependencyStoreStream = storage.getDependencyStoreStream();
  }

  @Override public Call<List<List<Span>>> getTraces(QueryRequest request) {
    return new GetTracesScatterCall(traceStoreStream, request);
  }

  @Override
  public Call<List<Span>> getTrace(String traceId) {
    return new GetTraceScatterCall(traceStoreStream, traceId);
  }

  @Deprecated @Override public Call<List<String>> getServiceNames() {
    return new GetServiceNamesScatterCall(traceStoreStream);
  }

  @Deprecated @Override public Call<List<String>> getSpanNames(String serviceName) {
    return new GetSpanNamesScatterCall(traceStoreStream, serviceName);
  }

  @Override public Call<List<String>> getRemoteServiceNames(String serviceName) {
    return new GetRemoteServiceNamesScatterCall(traceStoreStream, serviceName);
  }

  @Override public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
    return new GetDependenciesScatterCall(dependencyStoreStream, endTs, lookback);
  }

  static class GetServiceNamesScatterCall extends Call.Base<List<String>> {
    static final Gson GSON = new Gson();

    final KafkaStreams traceStoreStream;

    GetServiceNamesScatterCall(KafkaStreams traceStoreStream) {
      this.traceStoreStream = traceStoreStream;
    }

    @Override protected List<String> doExecute() throws IOException {
      return traceStoreStream.allMetadataForStore(SERVICE_NAMES_STORE_NAME)
          .parallelStream()
          .map(metadata -> HttpClient.of(
              String.format("http://%s:%d",
                  metadata.hostInfo().host(),
                  metadata.hostInfo().port())))
          .map(httpClient -> httpClient.get("/service_names")
              .aggregate()
              .join()
              .content(Charset.defaultCharset()))
          .map(response -> {
            Set<String> set = GSON.fromJson(response, Set.class);
            return set;
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
      return new GetServiceNamesScatterCall(traceStoreStream);
    }
  }

  static class GetSpanNamesScatterCall extends Call.Base<List<String>> {
    static final Gson GSON = new Gson();

    final KafkaStreams traceStoreStream;
    final String serviceName;

    GetSpanNamesScatterCall(KafkaStreams traceStoreStream, String serviceName) {
      this.traceStoreStream = traceStoreStream;
      this.serviceName = serviceName;
    }

    @Override protected List<String> doExecute() throws IOException {
      StreamsMetadata metadata =
          traceStoreStream.metadataForKey(SPAN_NAMES_STORE_NAME, serviceName,
              new StringSerializer());
      HttpClient httpClient = HttpClient.of(
          String.format("http://%s:%d",
              metadata.hostInfo().host(),
              metadata.hostInfo().port()));
      String content =
          httpClient.get(String.format("/service_names/%s/span_names", serviceName))
              .aggregate()
              .join()
              .content(Charset.defaultCharset());
      Set<String> set = GSON.fromJson(content, Set.class);
      return new ArrayList<>(set);
    }

    @Override protected void doEnqueue(Callback<List<String>> callback) {
      try {
        callback.onSuccess(doExecute());
      } catch (IOException e) {
        callback.onError(e);
      }
    }

    @Override public Call<List<String>> clone() {
      return new GetSpanNamesScatterCall(traceStoreStream, serviceName);
    }
  }

  static class GetRemoteServiceNamesScatterCall extends Call.Base<List<String>> {
    static final Gson GSON = new Gson();

    final KafkaStreams traceStoreStream;
    final String serviceName;

    GetRemoteServiceNamesScatterCall(KafkaStreams traceStoreStream, String serviceName) {
      this.traceStoreStream = traceStoreStream;
      this.serviceName = serviceName;
    }

    @Override protected List<String> doExecute() throws IOException {
      StreamsMetadata metadata =
          traceStoreStream.metadataForKey(REMOTE_SERVICE_NAMES_STORE_NAME, serviceName,
              new StringSerializer());
      HttpClient httpClient = HttpClient.of(
          String.format("http://%s:%d",
              metadata.hostInfo().host(),
              metadata.hostInfo().port()));
      String content =
          httpClient.get(String.format("/service_names/%s/remote_service_names", serviceName))
              .aggregate()
              .join()
              .content(Charset.defaultCharset());
      Set<String> set = GSON.fromJson(content, Set.class);
      return new ArrayList<>(set);
    }

    @Override protected void doEnqueue(Callback<List<String>> callback) {
      try {
        callback.onSuccess(doExecute());
      } catch (IOException e) {
        callback.onError(e);
      }
    }

    @Override public Call<List<String>> clone() {
      return new GetRemoteServiceNamesScatterCall(traceStoreStream, serviceName);
    }
  }

  static class GetTracesScatterCall extends Call.Base<List<List<Span>>> {
    static final Gson GSON = new Gson();

    final KafkaStreams traceStoreStream;
    final QueryRequest request;

    GetTracesScatterCall(KafkaStreams traceStoreStream, QueryRequest request) {
      this.traceStoreStream = traceStoreStream;
      this.request = request;
    }

    @Override protected List<List<Span>> doExecute() throws IOException {
      List<List<Span>> traces = traceStoreStream.allMetadataForStore(TRACES_STORE_NAME)
          .parallelStream()
          .map(metadata -> HttpClient.of(
              String.format("http://%s:%d",
                  metadata.hostInfo().host(),
                  metadata.hostInfo().port())))
          .map(httpClient -> httpClient.execute(
              RequestHeaders.of(HttpMethod.GET, "/traces"), GSON.toJson(request.toBuilder()))
              .aggregate()
              .join()
              .contentUtf8())
          .map(response -> {
            Traces result = GSON.fromJson(response, Traces.class);
            return result.traces;
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
      return new GetTracesScatterCall(traceStoreStream, request);
    }
  }

  static class GetTraceScatterCall extends Call.Base<List<Span>> {
    final KafkaStreams traceStoreStream;
    final String traceId;

    GetTraceScatterCall(KafkaStreams traceStoreStream, String traceId) {
      this.traceStoreStream = traceStoreStream;
      this.traceId = traceId;
    }

    @Override protected List<Span> doExecute() throws IOException {
      StreamsMetadata metadata =
          traceStoreStream.metadataForKey(TRACES_STORE_NAME, traceId, new StringSerializer());
      HttpClient httpClient = HttpClient.of(
          String.format("http://%s:%d",
              metadata.hostInfo().host(),
              metadata.hostInfo().port()));
      HttpData content =
          httpClient.get(String.format("/traces/%s", traceId))
              .aggregate()
              .join()
              .content();
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
      return new GetTraceScatterCall(traceStoreStream, traceId);
    }
  }

  static class GetDependenciesScatterCall extends Call.Base<List<DependencyLink>> {
    final KafkaStreams dependencyStoreStream;
    final long endTs, lookback;

    GetDependenciesScatterCall(KafkaStreams dependencyStoreStream,
        long endTs, long lookback) {
      this.dependencyStoreStream = dependencyStoreStream;
      this.endTs = endTs;
      this.lookback = lookback;
    }

    @Override protected List<DependencyLink> doExecute() throws IOException {
      return dependencyStoreStream.allMetadataForStore(DEPENDENCIES_STORE_NAME)
          .parallelStream()
          .map(metadata -> HttpClient.of(
              String.format("http://%s:%d",
                  metadata.hostInfo().host(),
                  metadata.hostInfo().port())))
          .map(httpClient -> httpClient.get(String.format("/dependencies?end_ts=%s&lookback=%s", endTs, lookback))
              .aggregate()
              .join()
              .content())
          .map(response -> {
            return DependencyLinkBytesDecoder.JSON_V1.decodeList(response.array());
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
      return new GetDependenciesScatterCall(dependencyStoreStream, endTs, lookback);
    }
  }
}
