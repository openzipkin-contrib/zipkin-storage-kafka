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
package zipkin2.storage.kafka.streams.serdes;

import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import zipkin2.DependencyLink;
import zipkin2.codec.DependencyLinkBytesDecoder;
import zipkin2.codec.DependencyLinkBytesEncoder;

public class DependencyLinksSerde implements Serde<List<DependencyLink>> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    // Nothing to configure
  }

  @Override
  public void close() {
    // No resources to close
  }

  @Override
  public Serializer<List<DependencyLink>> serializer() {
    return new DependencyLinksSerializer();
  }

  @Override
  public Deserializer<List<DependencyLink>> deserializer() {
    return new DependencyLinksDeserializer();
  }

  public static class DependencyLinksDeserializer
      implements Deserializer<List<DependencyLink>> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
      // Nothing to configure
    }

    @Override
    public List<DependencyLink> deserialize(String topic, byte[] data) {
      if (data == null) {
        return null;
      }
      return DependencyLinkBytesDecoder.JSON_V1.decodeList(data);
    }

    @Override
    public void close() {
      // No resources to close
    }
  }

  public static class DependencyLinksSerializer implements Serializer<List<DependencyLink>> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
      // Nothing to configure
    }

    @Override
    public byte[] serialize(String topic, List<DependencyLink> data) {
      if (data == null) {
        return null;
      }
      return DependencyLinkBytesEncoder.JSON_V1.encodeList(data);
    }

    @Override
    public void close() {
      // No resources to close
    }
  }
}
