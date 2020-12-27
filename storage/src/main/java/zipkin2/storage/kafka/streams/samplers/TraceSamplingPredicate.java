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

import java.util.Collection;
import java.util.function.Predicate;
import zipkin2.Span;
import zipkin2.internal.HexCodec;

public class TraceSamplingPredicate implements Predicate<Collection<Span>> {
  private final float rate;

  public TraceSamplingPredicate(float rate) {
    if (rate < 0.f || rate > 1.f)
      throw new IllegalArgumentException("rate should be between 0 and 1: was " + rate);

    this.rate = rate;
  }

  @Override public boolean test(Collection<Span> spans) {
    return spans.stream()
      .anyMatch(span -> {
        if (Boolean.TRUE.equals(span.debug())) return true;
        long traceId = HexCodec.lowerHexToUnsignedLong(span.traceId());
        // The absolute value of Long.MIN_VALUE is larger than a long, so Math.abs returns identity.
        // This converts to MAX_VALUE to avoid always dropping when traceId == Long.MIN_VALUE
        long t = traceId == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(traceId);
        return t <= (long) (Long.MAX_VALUE * rate);
      });
  }
}
