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

import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import zipkin2.DependencyLink;
import zipkin2.codec.DependencyLinkBytesDecoder;
import zipkin2.codec.DependencyLinkBytesEncoder;

public class DependencyLinkSerde implements Serde<DependencyLink> {
  static final String KEY_PATTERN = "%s:%s";

  public static String key(DependencyLink link) {
    return String.format(KEY_PATTERN, link.parent(), link.child());
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    // Nothing to configure
  }

  @Override
  public void close() {
    // No resources to close
  }

  @Override
  public Serializer<DependencyLink> serializer() {
    return new DependencyLinkSerializer();
  }

  @Override
  public Deserializer<DependencyLink> deserializer() {
    return new DependencyLinkDeserializer();
  }

  public static class DependencyLinkDeserializer
      implements Deserializer<DependencyLink> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
      // Nothing to configure
    }

    @Override
    public DependencyLink deserialize(String topic, byte[] data) {
      if (data == null) {
        return null;
      }
      return DependencyLinkBytesDecoder.JSON_V1.decodeOne(data);
    }

    @Override
    public void close() {
      // No resources to close
    }
  }

  public static class DependencyLinkSerializer implements Serializer<DependencyLink> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
      // Nothing to configure
    }

    @Override
    public byte[] serialize(String topic, DependencyLink data) {
      if (data == null) {
        return null;
      }
      return DependencyLinkBytesEncoder.JSON_V1.encode(data);
    }

    @Override
    public void close() {
      // No resources to close
    }
  }
}
