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

import no.sysco.middleware.kafka.util.StreamsTopologyGraphviz;
import org.apache.kafka.streams.Topology;

public class StreamGraphPrinter {
  public static void main(String[] args) {
    String spanTopicName = "zipkin-span-v1";
    String spanServicesTopicName = "zipkin-span-services-v1";
    String servicesTopicName = "zipkin-services-v1";
    String spanDependenciesTopicName = "zipkin-span-dependencies-v1";
    String dependenciesTopicName = "zipkin-dependencies-v1";

    System.out.println("# TRACE STORE TOPOLOGY");
    Topology traceStoreTopology = new TraceStoreStream(spanTopicName, spanTopicName,
        null).get();
    System.out.println(StreamsTopologyGraphviz.print(traceStoreTopology));
    System.out.println();

    System.out.println("# SERVICE AGGREGATION TOPOLOGY");
    Topology serviceAggregationTopology =
        new ServiceAggregationStream(spanServicesTopicName, servicesTopicName).get();
    System.out.println(StreamsTopologyGraphviz.print(serviceAggregationTopology));
    System.out.println();

    System.out.println("# SERVICE STORE TOPOLOGY");
    Topology serviceStoreTopology =
        new ServiceStoreStream(servicesTopicName, servicesTopicName).get();
    System.out.println(StreamsTopologyGraphviz.print(serviceStoreTopology));
    System.out.println();

    System.out.println("# DEPENDENCY AGGREGATION TOPOLOGY");
    Topology dependencyAggregationTopology =
        new DependencyAggregationStream(spanTopicName, spanDependenciesTopicName,
            dependenciesTopicName).get();
    System.out.println(StreamsTopologyGraphviz.print(dependencyAggregationTopology));
    System.out.println();

    System.out.println("# DEPENDENCY STORE TOPOLOGY");
    Topology dependencyStoreTopology =
        new DependencyStoreStream(dependenciesTopicName, dependenciesTopicName).get();
    System.out.println(StreamsTopologyGraphviz.print(dependencyStoreTopology));
  }
}
