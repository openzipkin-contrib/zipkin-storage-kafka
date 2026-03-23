/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.kafka.internal;

import java.util.List;
import zipkin2.Call;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.SpanStore;
import zipkin2.storage.Traces;

public class NoopSpanStore implements SpanStore, Traces {
  @Override public Call<List<List<Span>>> getTraces(QueryRequest queryRequest) {
    return Call.emptyList();
  }

  @Override public Call<List<Span>> getTrace(String s) {
    return Call.emptyList();
  }

  @Override public Call<List<List<Span>>> getTraces(Iterable<String> iterable) {
    return Call.emptyList();
  }

  @Override @Deprecated public Call<List<String>> getServiceNames() {
    return Call.emptyList();
  }

  @Override @Deprecated public Call<List<String>> getSpanNames(String s) {
    return Call.emptyList();
  }

  @Override public Call<List<DependencyLink>> getDependencies(long l, long l1) {
    return Call.emptyList();
  }
}
