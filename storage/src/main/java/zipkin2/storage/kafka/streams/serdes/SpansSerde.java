/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.kafka.streams.serdes;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.codec.SpanBytesEncoder;

public final class SpansSerde implements Serde<List<Span>> {
  @Override public Serializer<List<Span>> serializer() {
    return new SpansSerializer();
  }

  @Override public Deserializer<List<Span>> deserializer() {
    return new SpansDeserializer();
  }

  static final class SpansSerializer implements Serializer<List<Span>> {
    @Override public byte[] serialize(String topic, List<Span> data) {
      if (data == null) return null;
      return SpanBytesEncoder.PROTO3.encodeList(data);
    }
  }

  static final class SpansDeserializer implements Deserializer<List<Span>> {
    @Override public List<Span> deserialize(String topic, byte[] data) {
      if (data == null) return new ArrayList<>();
      return SpanBytesDecoder.PROTO3.decodeList(data);
    }
  }
}
