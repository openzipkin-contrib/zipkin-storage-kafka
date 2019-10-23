# Design

## Goals

* Provide a fast and reliable storage that enable extensibility via Kafka topics.
* Provide full storage functionality via streaming aggregations (e.g., spans queries, dependency graph).
* Create a processing space where additional enrichment can be plugged-in into the tracing pipeline.
* Remove need for additional storage when Kafka is available.

## Kafka Zipkin Storage

Storage is composed by 4 main components: 

- Span Consumer: repartition of collected span batches into individual spans keyed by `traceId`
- Span Aggregator: processing of _partitioned spans_ into aggregated traces and later into dependency links.
- Trace and Dependency Stores: building local state stores to support search and query APIs for traces and dependencies.

### Span Consumer

This component processes span batches, received via HTTP, Kafka, ActiveMQ, etc.; 
take each element and keyed them by `traceId` on the "zipkin-spans" topic for repartition.

| Property | Environment Variable | Description |
|----------|----------------------|-------------|
| `zipkin.storage.kafka.span-partitioning-enabled` | `SPAN_PARTITIONING_ENABLED` | `false` disables span partitioning. Consider that `Aggregation` component requires partitioned spans. Defaults to `true`. |
| `zipkin.storage.kafka.partitioned-spans-topic` | `none` | Kafka topic where partitioned spans will be stored. Defaults to `zipkin-spans` |

> This component is currently compensating how `KafkaSender` (part of [Zipkin-Reporter](https://github.com/openzipkin/zipkin-reporter-java))
is reporting spans to Kafka, by grouping spans into batches and sending them to a un-keyed
Kafka topic.

Source code: [KafkaSpanConsumer.java](storage/src/main/java/zipkin2/storage/kafka/KafkaSpanConsumer.java)

### Span Aggregator

Partitioned spans are processed to produced two aggregated streams: `trace` and `dependency`.

#### Trace Stream 

Spans are grouped by ID and stored on a local
[Session window](https://kafka.apache.org/23/javadoc/org/apache/kafka/streams/kstream/SessionWindows.html),
where the `traceId` becomes the token, and `trace-timeout` (default: 1 minute)
(i.e. period of time without receiving a span with the same session; also known as session inactivity gap
in Kafka Streams) 
defines if a trace is still active or not. This is evaluated on the next span received on the stream--
regardless of incoming `traceId`. If session window is closed, a trace message is emitted to the 
traces topic.

![Session Windows](https://kafka.apache.org/20/images/streams-session-windows-02.png)

> Each color represents a trace. The longer `trace timeout` we have, the longer we wait 
to close a window and the longer we wait to emit traces downstream for dependency link and additional
aggregations; but also the more consistent the trace aggregation is.
If we choose a smaller gap, then we emit traces faster with the risk of breaking traces into 
smaller chunks, and potentially affecting counters downstream.

#### Dependency Stream

Once `traces` are emitted downstream as part of the initial processing, dependency links are evaluated
on each trace, and emitted the dependencies topic for further metric aggregation.

| Property | Environment Variable | Description |
|----------|----------------------|-------------|
| `zipkin.storage.kafka.span-aggregation-enabled` | `SPAN_AGGREGATION_ENABLED` | `false` disables span aggregation. Consider that `Store` components will require traces and dependency streams as input. Defaults to `true`. |
| `zipkin.storage.kafka.aggregation-spans-topic` | `none` | Kafka topic where partitioned spans will be consumed from. Key type: trace-id string. Value type: binary list of spans. Defaults to `zipkin-spans` |
| `zipkin.storage.kafka.aggregation-trace-topic` | `none` | Kafka topic where aggregated traces will be stored. Key type: trace-id string. Value type: binary list of spans. Defaults to `zipkin-trace` |
| `zipkin.storage.kafka.aggregation-dependency-topic` | `none` | Kafka topic where aggregated dependencies will be stored. Key type: string with `parent&#124;child` format. Value type: dependency link. Defaults to `zipkin-dependency` |

Kafka Streams topology: ![trace aggregation](docs/trace-aggregation-topology.png)

### Trace Store

This component build local stores from state received on `spans` Kafka topic 
for traces, service names and autocomplete tags. 

This component supports search and query APIs on top of local state stores build by the Store 
Kafka Streams component.

#### Get Service Names/Get Span Names/Get Remote Service Names

These queries are supported by service names indexed stores built from `spans` Kafka topic.

Store names:

- `zipkin-service-names`: key/value store with service name as key and value.
- `zipkin-span-names`: key/value store with service name as key and span names list as value.
- `zipkin-remote-service-names`: key/value store with service name as key and remote service names as value.

#### Get Trace/Find Traces

These queries are supported by two key value stores: 

- `zipkin-traces`: indexed by `traceId`, contains span list status received from `spans` Kafka topic.
- `zipkin-traces-by-timestamp`: list of trace IDs indexed by `timestamp`.

`GetTrace` query is supported by `zipkin-traces` store.
`FindTraces` query is supported by both: When receiving a query request time range is used to get
trace IDs, and then query request is tested on each trace to build a response.

#### Get Keys/Get Values

Supported by a key-value containing list of values valid for `autocompleteKeys`.

- `zipkin-autocomplete-tags`: key-value store.

| Property | Environment Variable | Description |
|----------|----------------------|-------------|
| `zipkin.storage.kafka.trace-by-id-query-enabled` | `TRACE_BY_ID_QUERY_ENABLED` | `false` disables query by trace ID `GET /traces/{trace_id}`. Defaults to `true`. |
| `zipkin.storage.kafka.trace-search-enabled` | `TRACE_SEARCH_ENABLED` | `false` disables trace search functionality `GET /traces` and indexes: service names, span names, tags. Defaults to `true`. |
| `zipkin.storage.kafka.storage-spans-topic` | `none` | Kafka topic where trace spans are consumed from to build state store. Expected value: list of spans; it does not use record keys. Defaults to `zipkin-spans` |

Source code: [TraceStoreTopology](storage/src/main/java/zipkin2/storage/kafka/streams/TraceStoreTopology.java)

Kafka Streams topology: ![trace store](docs/trace-store-topology.png)


### Dependency Store

This component build local state store from dependency stream. It builds a 1 minute time-window when counts calls and errors. When a request is received, time range is used to pick valid windows and join counters.

Windowed store: `zipkin-dependencies`.

| Property | Environment Variable | Description |
|----------|----------------------|-------------|
| `zipkin.storage.kafka.dependency-query-enabled` | `DEPENDENCY_QUERY_ENABLED` | `false` disables dependency query store `GET /dependencies`. Consider that `Aggregation` component requires partitioned spans. Defaults to `true`. |
| `zipkin.storage.kafka.storage-dependency-topic` | `none` | Kafka topic where dependencies are consumed from to build state store. Defaults to `zipkin-dependency` |

Kafka Streams topology: ![dependency store](docs/dependency-store-topology.png)

Source code: [DependencyStoreTopologySupplier](storage/src/main/java/zipkin2/storage/kafka/streams/DependencyStoreTopology.java)
