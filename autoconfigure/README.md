# Configuration

## Storage configurations

| Configuration | Description | Default |
|---------------|-------------|---------|
| `KAFKA_STORE_SPAN_CONSUMER_ENABLED` | Process spans collected by Zipkin server | `true` |
| `KAFKA_STORE_AGGREGATION_ENABLED` | Aggregate and store Zipkin data | `true` |
| `KAFKA_STORE_BOOTSTRAP_SERVERS` | Kafka bootstrap servers, format: `host:port` | `localhost:9092` |
| `KAFKA_STORE_ENSURE_TOPICS` | Ensure topics are created if don't exist | `true` |
| `KAFKA_STORE_DIRECTORY` | Root path where Zipkin stores tracing data | `/tmp/zipkin` |
| `KAFKA_STORE_COMPRESSION_TYPE` | Compression type used to store data in Kafka topics | `NONE` |
| `KAFKA_STORE_RETENTION_SCAN_FREQUENCY` | Frequency to scan old records, in milliseconds. | `86400000` (1 day) |
| `KAFKA_STORE_RETENTION_MAX_AGE` | Max age of a trace, to recognize old one for retention policies. | `604800000` (7 day) |

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

> Use partitions and replication factor when Topics are created by Zipkin. If topics are created manually
those options are not used.
