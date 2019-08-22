# Configuration

## Broker and topics

| Configuration | Description | Default |
|---------------|-------------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap servers | `localhost:9092` |
| `KAFKA_SPANS_TOPIC` | Topic where incoming spans are stored. | `zipkin-spans` |
| `KAFKA_TRACE_TOPIC` | Topic where aggregated traces are stored. | `zipkin-trace` |
| `KAFKA_DEPENDENCY_TOPIC` | Topic where aggregated service dependencies names are stored. | `zipkin-dependency` |

## Storage configurations

| Configuration | Description | Default |
|---------------|-------------|---------|
| `KAFKA_STORAGE_DIR` | Root path where Zipkin stores tracing data | `/data` |
| `KAFKA_STORAGE_TRACE_INACTIVITY_GAP` | How long to wait until a trace window is closed (ms). If this config is to small, dependency links won't be caught and metrics may drift. | `600000` (1 minute) |
| `KAFKA_STORAGE_TRACE_TTL` | How long to keep traces stored. | `259200000` (3 days) |
| `KAFKA_STORAGE_DEPENDENCY_TTL` | How long to keep dependencies stored. | `604800000` (1 week) |
