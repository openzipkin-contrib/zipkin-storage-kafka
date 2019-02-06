package zipkin2.storage.kafka.internal;

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
