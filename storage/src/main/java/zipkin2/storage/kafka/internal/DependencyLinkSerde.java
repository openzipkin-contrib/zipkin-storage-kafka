package zipkin2.storage.kafka.internal;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import zipkin2.DependencyLink;
import zipkin2.codec.DependencyLinkBytesDecoder;
import zipkin2.codec.DependencyLinkBytesEncoder;

import java.util.Map;

public class DependencyLinkSerde implements Serde<DependencyLink> {

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
			if (data == null)
				return null;
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
			if (data == null)
				return null;
			return DependencyLinkBytesEncoder.JSON_V1.encode(data);
		}

		@Override
		public void close() {
			// No resources to close
		}

	}

}
