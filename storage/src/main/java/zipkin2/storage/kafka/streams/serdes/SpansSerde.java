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

import java.util.ArrayList;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.codec.SpanBytesEncoder;

import java.util.List;
import java.util.Map;

public class SpansSerde implements Serde<List<Span>> {

  private final SpanBytesDecoder spanBytesDecoder;

  private final SpanBytesEncoder spanBytesEncoder;

  public SpansSerde() {
    this.spanBytesDecoder = SpanBytesDecoder.PROTO3;
    this.spanBytesEncoder = SpanBytesEncoder.PROTO3;
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
  public Serializer<List<Span>> serializer() {
    return new SpansSerializer();
  }

  @Override
  public Deserializer<List<Span>> deserializer() {
    return new SpansDeserializer();
  }

  private class SpansSerializer implements Serializer<List<Span>> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
      // Nothing to configure
    }

    @Override
    public byte[] serialize(String topic, List<Span> data) {
      if (data == null) return null;
      return spanBytesEncoder.encodeList(data);
    }

    @Override
    public void close() {
      // No resources to close
    }
  }

  private class SpansDeserializer implements Deserializer<List<Span>> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
      // Nothing to configure
    }

    @Override
    public List<Span> deserialize(String topic, byte[] data) {
      if (data == null) return new ArrayList<>();
      return spanBytesDecoder.decodeList(data);
    }

    @Override
    public void close() {
      // No resources to close
    }
  }
}
