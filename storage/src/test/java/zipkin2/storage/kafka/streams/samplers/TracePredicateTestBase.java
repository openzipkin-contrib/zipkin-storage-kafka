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
 *
 */

package zipkin2.storage.kafka.streams.samplers;

import org.junit.Ignore;
import zipkin2.Span;

@Ignore
class TracePredicateTestBase {
  final Span errorSpanOfTrace1 = errorSpan("1");
  final Span normalSpanOfTrace1 = normalSpan("1");

  private Span errorSpan(String traceId) {
    return Span.newBuilder()
      .traceId(traceId).id("1").name("get").duration(0L)
      .putTag("error", "500")
      .build();
  }

  private Span normalSpan(String traceId) {
    return Span.newBuilder()
      .traceId(traceId).id("2").name("get").duration(0L)
      .build();
  }

  Span debugSpan(String traceId) {
    return Span.newBuilder()
      .traceId(traceId).id("3").name("get").duration(0L).debug(true)
      .build();
  }

  Span slowSpan(String traceId) {
    return Span.newBuilder()
      .traceId(traceId).id("4").name("get").duration(10_000_000L)
      .build();
  }
}
