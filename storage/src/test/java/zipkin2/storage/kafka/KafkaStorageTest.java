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

import java.util.Arrays;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class KafkaStorageTest {

  @Test
  public void notSupported() {
    try {
      KafkaStorage.newBuilder().strictTraceId(false);
      fail();
    } catch (IllegalArgumentException ignored) {
    }

    try {
      KafkaStorage.newBuilder().searchEnabled(true);
      fail();
    } catch (IllegalArgumentException ignored) {
    }

    try {
      KafkaStorage.newBuilder().autocompleteKeys(null);
      fail();
    } catch (NullPointerException ignored) {
    }

    try {
      KafkaStorage.newBuilder().autocompleteKeys(Arrays.asList("key1", "key2"));
      fail();
    } catch (IllegalArgumentException ignored) {
    }
  }

  @Test
  public void buildDefaultBuilder() {
    KafkaStorage.Builder builder = KafkaStorage.newBuilder();
    assertNotNull(builder.storeDirectory);

    try {
      builder.spansTopic(null);
      fail();
    } catch (NullPointerException ignored) {
    }

    try {
      builder.dependencyLinksTopic(null);
      fail();
    } catch (NullPointerException ignored) {
    }

    try {
      builder.storeDirectory(null);
      fail();
    } catch (NullPointerException ignored) {
    }
  }

  @Test
  public void topicDefault() {
    try {
      KafkaStorage.Topic.builder(null);
      fail();
    } catch (NullPointerException ignored) {
    }

    KafkaStorage.Topic.Builder topicBuilder = KafkaStorage.Topic.builder("topic-1");

    try {
      topicBuilder.partitions(0);
      fail();
    } catch (IllegalArgumentException ignored) {
    }

    try {
      topicBuilder.partitions(null);
      fail();
    } catch (NullPointerException ignored) {
    }

    try {
      topicBuilder.partitions(-1);
      fail();
    } catch (IllegalArgumentException ignored) {
    }

    try {
      topicBuilder.replicationFactor(null);
      fail();
    } catch (NullPointerException ignored) {
    }

    try {
      topicBuilder.replicationFactor((short) 0);
      fail();
    } catch (IllegalArgumentException ignored) {
    }
  }
}
