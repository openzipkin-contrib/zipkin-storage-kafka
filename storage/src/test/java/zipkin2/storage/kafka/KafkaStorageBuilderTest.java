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
 */
package zipkin2.storage.kafka;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaStorageBuilderTest {

  @Test void notSupported() {
    assertThatThrownBy(() -> KafkaStorage.newBuilder().strictTraceId(false))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test void buildDefaultBuilder() {
    KafkaStorageBuilder builder = KafkaStorage.newBuilder();
    assertThat(builder.storageStateDir).isNotNull();

    assertThatThrownBy(() -> builder.spanPartitioning.spansTopic(null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> builder.spanAggregation.spansTopic(null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> builder.spanAggregation.traceTopic(null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> builder.spanAggregation.dependencyTopic(null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> builder.traceStorage.spansTopic(null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> builder.dependencyStorage.dependencyTopic(null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> builder.storageStateDir(null))
        .isInstanceOf(NullPointerException.class);
  }
}
