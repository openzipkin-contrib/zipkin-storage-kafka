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

import org.junit.Test;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class CompositeTracePredicateTest extends TracePredicateTestBase {
  @Test
  public void acceptIfAnyTestPassed() {
    final CompositeTracePredicate predicate = new CompositeTracePredicate(
      asList(spans -> false, spans -> true)
    );

    final boolean sampled = predicate.test(asList(errorSpanOfTrace1, normalSpanOfTrace1));

    assertThat(sampled).isTrue();
  }

  @Test
  public void discardIfAllTestFailed() {
    final CompositeTracePredicate predicate = new CompositeTracePredicate(
      asList(spans -> false, spans -> false)
    );

    final boolean sampled = predicate.test(asList(errorSpanOfTrace1, normalSpanOfTrace1));

    assertThat(sampled).isFalse();
  }
}
