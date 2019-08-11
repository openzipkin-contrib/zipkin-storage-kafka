# Design

## Goals

* Provide a fast and reliable storage that enable extensibility via Kafka Topics.
* Provide full storage functionality via streaming aggregations (e.g., dependency graph).
* Create a processing space where additional enrichment can be plugged in into the processing 
pipeline.
* Remove need for additional storage when Kafka is available.

### Zipkin Storage Component

A Zipkin Storage component has the following internal parts:

* `Builder`: which configures if
    - `strictTraceId(boolean strictTraceId)`
    - `searchEnabled(boolean searchEnabled)`
    - `autocompleteKeys(List<String> keys)`
    - `autocompleteTtl(int autocompleteTtl)`
    - `autocompleteCardinality(int autocompleteCardinality)`
* `SpanStore`: main component
    - `Call<List<List<Span>>> getTraces(QueryRequest request);`
    - `Call<List<Span>> getTrace(String traceId);`
    - `Call<List<String>> getServiceNames();`
    - `Call<List<String>> getSpanNames(String serviceName);`
    - `Call<List<DependencyLink>> getDependencies(long endTs, long lookback);`
* `SpanConsumer`: which ingest spans
    - `Call<Void> accept(List<Span> spans)`
* `QueryRequest`: which includes
    - `String serviceName, spanName;`
    - `Map<String, String> annotationQuery;`
    - `Long minDuration, maxDuration;`
    - `long endTs, lookback;`
    - `int limit;`

### Kafka Zipkin Storage

#### `KafkaSpanStore`

Span Store is expecting Spans to be stored in topics partitioned by `TraceId`.

> These can be created by Span Consumer, or can be **enriched** by other Stream Processors, outside of
Zipkin Server.

Kafka Span Store will need to support different kind of queries:


##### Get Service Names/Get Span Names

Service name to Span names pairs are indexed by aggregating spans.

##### Get Trace/Find Traces

When search requests are received, span index is used to search for trace ids. After a list is 
retrieved, trace DAG is retrieved from trace state store.

##### Get Dependencies

After `spans` are aggregated into traces, traces are processed to collect dependencies. 
Dependencies changelog are stored in a Kafka topic to be be stored as materialized view on 
Zipkin instances.

### Stream processors

#### Trace Aggregation Stream Processor

This is the main processor that take incoming spans and aggregate them into:

- Traces
- Dependencies

![trace aggregation](docs/trace-aggregation-topology.png)

#### Store Stream Processor

Kafka Stream store tables for traces, service names and dependencies to be available on local state.

![trace store](docs/trace-store-topology.png)
