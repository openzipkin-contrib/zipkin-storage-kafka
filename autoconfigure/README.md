# Configuration

## Storage configurations

| Configuration | Description | Default |
|---------------|-------------|---------|
| `KAFKA_STORE_SPAN_CONSUMER_ENABLED` | Enable partitioning of incoming span batches into individual spans keyed by trace ID. | `true` |
| `KAFKA_STORE_AGGREGATION_ENABLED` | Enable aggregation spans into traces and then into dependency links. Currently this has to be initiated with span consumer or storage, or initiate manually via `curl /health` | `true` |
| `KAFKA_STORE_SPAN_STORE_ENABLED` | Enable storage for spans, traces, dependencies, tags, and service names. | `true` |
| `KAFKA_STORE_BOOTSTRAP_SERVERS` | Kafka bootstrap servers | `localhost:9092` |
| `KAFKA_STORE_ENSURE_TOPICS` | Ensure topics are created if don't exist. Is recommended to create them manually, to define partition and replication according to your environment. | `true` |
| `KAFKA_STORE_COMPRESSION_TYPE` | Compression type used to store data in Kafka topics | `NONE` |
| `KAFKA_STORE_DIRECTORY` | Root path where Zipkin stores tracing data | `/tmp/zipkin` |

## Storage durations and timeouts

| Configuration | Description | Default |
|---------------|-------------|---------|
| `KAFKA_STORE_TRACES_RETENTION_SCAN_FREQUENCY` | How often to validate traces retention. This will be evaluated at the time events are received. (ms) | `3600000` (1 hour) |
| `KAFKA_STORE_TRACES_RETENTION_PERIOD` | How long to keep traces stored. | `604800000` (1 week) |
| `KAFKA_STORE_TRACES_INACTIVITY_GAP` | How long to wait until a trace is marked as done (ms). This affects dependency links and traces indexed, but query join traces part to complete results. | `30000` (30 seconds) |
| `KAFKA_STORE_DEPENDENCIES_RETENTION_PERIOD` | How long to keep dependencies stored. | `604800000` (1 week) |

## Topics configuration

| Configuration | Description | Default |
| `KAFKA_STORE_SPANS_TOPIC` | Topic where incoming spans are stored. | `zipkin-spans` |
| `KAFKA_STORE_SPANS_TOPIC_PARTITIONS` | Span topic number of partitions. | `1` |
| `KAFKA_STORE_SPANS_TOPIC_REPLICATION_FACTOR` | Span topic replication factor. | `1` |
| `KAFKA_STORE_TRACES_TOPIC` | Topic where aggregated traces are stored. | `zipkin-traces` |
| `KAFKA_STORE_TRACES_TOPIC_PARTITIONS` | Traces topic number of partitions. | `1` |
| `KAFKA_STORE_TRACES_TOPIC_REPLICATION_FACTOR` | Traces topic replication factor. | `1` |
| `KAFKA_STORE_DEPENDENCIES_TOPIC` | Topic where aggregated service dependencies names are stored. | `zipkin-dependencies` |
| `KAFKA_STORE_DEPENDENCIES_TOPIC_PARTITIONS` | Services topic number of partitions. | `1` |
| `KAFKA_STORE_DEPENDENCIES_TOPIC_REPLICATION_FACTOR` | Services topic replication factor. | `1` |

