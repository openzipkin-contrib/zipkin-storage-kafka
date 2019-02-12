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
package zipkin2.storage.kafka.internal.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;

public class SpanNamesSerde implements Serde<Set<String>> {
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    //Nothing to do.
  }

  @Override
  public void close() {
  }

  @Override
  public Serializer<Set<String>> serializer() {
    return new SpanNamesSerializer();
  }

  @Override
  public Deserializer<Set<String>> deserializer() {
    return new SpanNamesDeserializer();
  }

  public static class SpanNamesSerializer implements Serializer<Set<String>> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
      //Nothing to do.
    }

    @Override
    public byte[] serialize(String topic, Set<String> data) {
      String values = String.join("|", data);
      return values.getBytes(UTF_8);
    }

    @Override
    public void close() {
    }
  }

  public static class SpanNamesDeserializer implements Deserializer<Set<String>> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
      //Nothing to do.
    }

    @Override
    public Set<String> deserialize(String topic, byte[] data) {
      String decoded = new String(data, UTF_8);
      String[] values = decoded.split("\\|");
      return new HashSet<>(Arrays.asList(values));
    }

    @Override
    public void close() {
    }
  }
}
