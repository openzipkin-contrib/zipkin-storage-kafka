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
package zipkin2.storage.kafka.streams.serdes;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class NamesSerde implements Serde<Set<String>> {
  @Override public Serializer<Set<String>> serializer() {
    return new SpanNamesSerializer();
  }

  @Override public Deserializer<Set<String>> deserializer() {
    return new SpanNamesDeserializer();
  }

  static final class SpanNamesSerializer implements Serializer<Set<String>> {
    @Override public byte[] serialize(String topic, Set<String> data) {
      String values = String.join("|", data);
      return values.getBytes(UTF_8);
    }
  }

  static final class SpanNamesDeserializer implements Deserializer<Set<String>> {
    @Override public Set<String> deserialize(String topic, byte[] data) {
      String decoded = new String(data, UTF_8);
      String[] values = decoded.split("\\|");
      return new LinkedHashSet<>(Arrays.asList(values));
    }
  }
}
