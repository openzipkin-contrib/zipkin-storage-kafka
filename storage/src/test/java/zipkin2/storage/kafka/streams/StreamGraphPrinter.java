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
import no.sysco.middleware.kafka.util.StreamsTopologyGraphviz;
import org.apache.kafka.streams.Topology;

public class StreamGraphPrinter {
  public static void main(String[] args) {
    String spansTopic = "topic";
    String tracesTopic = "traces";
    String traceSpansTopic = "trace-spans";
    String traceStoreName = "trace-store";
    String serviceStoreName = "service-store";
    String dependencyStoreName = "dependencies";
    String spanIndexStoreName = "span-index";
    String dependenciesTopic = "dependencies";

    System.out.println();
    System.out.println("# SPAN CONSUMER TOPOLOGY");
    Topology spanConsumerTopology =
        new SpanConsumerStream(spansTopic, tracesTopic).get();
    System.out.println(StreamsTopologyGraphviz.print(spanConsumerTopology));

    System.out.println();
    System.out.println("# SERVICE STORE TOPOLOGY");
    Topology serviceStoreTopology = new ServiceStoreStream(traceSpansTopic, serviceStoreName).get();
    System.out.println(StreamsTopologyGraphviz.print(serviceStoreTopology));

    System.out.println();
    System.out.println("# SPAN INDEX TOPOLOGY");
    Topology spanIndexTopology = new SpanIndexStream(spansTopic, spanIndexStoreName, "").get();
    System.out.println(StreamsTopologyGraphviz.print(spanIndexTopology));

    System.out.println();
    System.out.println("# TRACE STORE TOPOLOGY");
    Topology traceStoreTopology = new TraceStoreStream(traceSpansTopic, traceStoreName).get();
    System.out.println(StreamsTopologyGraphviz.print(traceStoreTopology));

    System.out.println();
    System.out.println("# RETENTION TOPOLOGY");
    Topology traceRetentionTopology =
        new TraceRetentionStoreStream(traceSpansTopic, traceStoreName, Duration
            .ofMinutes(1), Duration.ofMinutes(1)).get();
    System.out.println(StreamsTopologyGraphviz.print(traceRetentionTopology));

    System.out.println();
    System.out.println("# TRACE AGGREGATION TOPOLOGY");
    Topology traceAggregationTopology =
        new TraceAggregationStream(traceSpansTopic, traceStoreName, tracesTopic,
            dependenciesTopic, Duration.ofMinutes(5)).get();
    System.out.println(StreamsTopologyGraphviz.print(traceAggregationTopology));

    System.out.println();
    System.out.println("# DEPENDENCY TOPOLOGY");
    Topology dependencyStoreTopology =
        new DependencyStoreStream(dependenciesTopic, dependencyStoreName).get();
    System.out.println(StreamsTopologyGraphviz.print(dependencyStoreTopology));
  }
}
