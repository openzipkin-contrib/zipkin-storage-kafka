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
import java.io.IOException;
import java.util.List;
import org.apache.kafka.streams.KafkaStreams;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.codec.DependencyLinkBytesDecoder;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.ServiceAndSpanNames;
import zipkin2.storage.SpanStore;
import zipkin2.storage.kafka.internal.KafkaStoreScatterGatherListCall;
import zipkin2.storage.kafka.internal.KafkaStoreSingleKeyListCall;
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

  static class GetServiceNamesCall extends KafkaStoreScatterGatherListCall<String> {
    final KafkaStreams traceStoreStream;

    GetServiceNamesCall(KafkaStreams traceStoreStream) {
      super(traceStoreStream, SERVICE_NAMES_STORE_NAME, "/serviceNames");
      this.traceStoreStream = traceStoreStream;
    }

    @Override protected String parse(JsonNode node) {
      return node.textValue();
    }

    @Override public Call<List<String>> clone() {
      return new GetServiceNamesCall(traceStoreStream);
    }
  }

  static class GetSpanNamesCall extends KafkaStoreSingleKeyListCall<String> {
    final KafkaStreams traceStoreStream;
    final String serviceName;

    GetSpanNamesCall(KafkaStreams traceStoreStream, String serviceName) {
      super(traceStoreStream, SPAN_NAMES_STORE_NAME,
          String.format("/serviceNames/%s/spanNames", serviceName), serviceName);
      this.traceStoreStream = traceStoreStream;
      this.serviceName = serviceName;
    }

    @Override protected String parse(JsonNode node) {
      return node.textValue();
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

  static class GetRemoteServiceNamesCall extends KafkaStoreSingleKeyListCall<String> {
    final KafkaStreams traceStoreStream;
    final String serviceName;

    GetRemoteServiceNamesCall(KafkaStreams traceStoreStream, String serviceName) {
      super(traceStoreStream, REMOTE_SERVICE_NAMES_STORE_NAME,
          String.format("/serviceNames/%s/remoteServiceNames", serviceName), serviceName);
      this.traceStoreStream = traceStoreStream;
      this.serviceName = serviceName;
    }

    @Override protected String parse(JsonNode node) {
      return node.textValue();
    }

    @Override public Call<List<String>> clone() {
      return new GetRemoteServiceNamesCall(traceStoreStream, serviceName);
    }
  }

  static class GetTracesCall extends KafkaStoreScatterGatherListCall<List<Span>> {
    final KafkaStreams traceStoreStream;
    final QueryRequest request;

    GetTracesCall(KafkaStreams traceStoreStream, QueryRequest request) {
      super(traceStoreStream, TRACES_STORE_NAME, String.format("/traces?%s%s%s%s%s%s%s%s%s",
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
          "limit=" + request.limit()));
      this.traceStoreStream = traceStoreStream;
      this.request = request;
    }

    @Override protected List<Span> parse(JsonNode node) {
      return SpanBytesDecoder.JSON_V2.decodeList(node.toString().getBytes());
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

  static class GetTraceCall extends KafkaStoreSingleKeyListCall<Span> {
    final KafkaStreams traceStoreStream;
    final String traceId;

    GetTraceCall(KafkaStreams traceStoreStream, String traceId) {
      super(traceStoreStream, TRACES_STORE_NAME, String.format("/traces/%s", traceId), traceId);
      this.traceStoreStream = traceStoreStream;
      this.traceId = traceId;
    }

    @Override protected Span parse(JsonNode node) {
      return SpanBytesDecoder.JSON_V2.decodeOne(node.toString().getBytes());
    }

    @Override public Call<List<Span>> clone() {
      return new GetTraceCall(traceStoreStream, traceId);
    }
  }

  static class GetDependenciesCall extends KafkaStoreScatterGatherListCall<DependencyLink> {
    final KafkaStreams dependencyStoreStream;
    final long endTs, lookback;

    GetDependenciesCall(KafkaStreams dependencyStoreStream, long endTs, long lookback) {
      super(dependencyStoreStream, DEPENDENCIES_STORE_NAME,
          String.format("/dependencies?endTs=%s&lookback=%s", endTs, lookback));
      this.dependencyStoreStream = dependencyStoreStream;
      this.endTs = endTs;
      this.lookback = lookback;
    }

    @Override protected DependencyLink parse(JsonNode node) {
      return DependencyLinkBytesDecoder.JSON_V1.decodeOne(node.toString().getBytes());
    }

    @Override public Call<List<DependencyLink>> clone() {
      return new GetDependenciesCall(dependencyStoreStream, endTs, lookback);
    }
  }
}
