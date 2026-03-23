/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.kafka;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaStorageBuilderTest {

  @Test void notSupported() {
    assertThatThrownBy(() -> KafkaStorage.newBuilder().strictTraceId(false))
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test void buildDefaultBuilder() {
    KafkaStorageBuilder builder = KafkaStorage.newBuilder();

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
