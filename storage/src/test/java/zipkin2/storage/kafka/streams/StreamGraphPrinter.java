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
package zipkin2.storage.kafka.streams;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import no.sysco.middleware.kafka.util.StreamsTopologyGraphviz;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

public class StreamGraphPrinter {
  public static void main(String[] args) {
    // given
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    props.put(StreamsConfig.STATE_DIR_CONFIG,
        "target/kafka-streams-test/" + Instant.now().getEpochSecond());
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);

    String spansTopic = "topic";
    String tracesTopic = "traces";
    String traceSpansTopic = "trace-spans";
    String traceStoreName = "trace-store";
    String servicesTopic = "services";
    String serviceStoreName = "service-store";
    String dependencyStoreName = "dependencies";
    String spanIndexStoreName = "span-index";

    Topology spanConsumerTopology =
        new SpanConsumerStream(spansTopic, servicesTopic, tracesTopic).get();
    System.out.println(StreamsTopologyGraphviz.print(spanConsumerTopology));

    Topology traceAggregationTopology =
        new TraceAggregationStream(traceSpansTopic, traceStoreName, tracesTopic).get();
    System.out.println(StreamsTopologyGraphviz.print(traceAggregationTopology));

    Topology serviceStoreTopology = new ServiceStoreStream(servicesTopic, serviceStoreName).get();
    System.out.println(StreamsTopologyGraphviz.print(serviceStoreTopology));

    Topology dependencyStoreTopology =
        new DependencyStoreStream(tracesTopic, dependencyStoreName).get();
    System.out.println(StreamsTopologyGraphviz.print(dependencyStoreTopology));

    Topology spanIndexTopology = new SpanIndexStream(spansTopic, spanIndexStoreName, "").get();
    System.out.println(StreamsTopologyGraphviz.print(spanIndexTopology));

    Topology traceStoreTopology = new TraceStoreStream(traceSpansTopic, traceStoreName).get();
    System.out.println(StreamsTopologyGraphviz.print(traceStoreTopology));

    Topology traceRetentionTopology =
        new TraceRetentionStoreStream(traceSpansTopic, traceStoreName, Duration
            .ofMinutes(1), Duration.ofMinutes(1)).get();
    System.out.println(StreamsTopologyGraphviz.print(traceRetentionTopology));

  }
}
