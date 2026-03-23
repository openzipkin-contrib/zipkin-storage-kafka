/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.kafka.streams.serdes;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class SpanIdsSerde implements Serde<Set<String>> {
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
