package zipkin2.storage.kafka.internal;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.codec.SpanBytesEncoder;

import java.util.Map;

public class SpanSerde implements Serde<Span> {

	private final SpanBytesDecoder spanBytesDecoder;

	private final SpanBytesEncoder spanBytesEncoder;

	public SpanSerde() {
		spanBytesDecoder = SpanBytesDecoder.PROTO3;
		spanBytesEncoder = SpanBytesEncoder.PROTO3;
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
	public Serializer<Span> serializer() {
		return new SpanSerializer();
	}

	@Override
	public Deserializer<Span> deserializer() {
		return new SpanDeserializer();
	}

	private class SpanSerializer implements Serializer<Span> {

		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {
			// Nothing to configure
		}

		@Override
		public byte[] serialize(String topic, Span data) {
			return spanBytesEncoder.encode(data);
		}

		@Override
		public void close() {
			// No resources to close
		}

	}

	private class SpanDeserializer implements Deserializer<Span> {

		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {
			// Nothing to configure
		}

		@Override
		public Span deserialize(String topic, byte[] data) {
			return spanBytesDecoder.decodeOne(data);
		}

		@Override
		public void close() {
			// No resources to close
		}

	}

}
