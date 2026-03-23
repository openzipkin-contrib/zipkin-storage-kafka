/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.kafka.streams.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import zipkin2.DependencyLink;
import zipkin2.codec.DependencyLinkBytesDecoder;
import zipkin2.codec.DependencyLinkBytesEncoder;

public final class DependencyLinkSerde implements Serde<DependencyLink> {
  static final String KEY_PATTERN = "%s:%s";

  public static String linkKey(DependencyLink link) {
    return KEY_PATTERN.formatted(link.parent(), link.child());
  }

  @Override public Serializer<DependencyLink> serializer() {
    return new DependencyLinkSerializer();
  }

  @Override public Deserializer<DependencyLink> deserializer() {
    return new DependencyLinkDeserializer();
  }

  static final class DependencyLinkDeserializer implements Deserializer<DependencyLink> {
    @Override public DependencyLink deserialize(String topic, byte[] data) {
      if (data == null) return null;
      return DependencyLinkBytesDecoder.JSON_V1.decodeOne(data);
    }
  }

  static final class DependencyLinkSerializer implements Serializer<DependencyLink> {
    @Override public byte[] serialize(String topic, DependencyLink data) {
      if (data == null) return null;
      return DependencyLinkBytesEncoder.JSON_V1.encode(data);
    }
  }
}
