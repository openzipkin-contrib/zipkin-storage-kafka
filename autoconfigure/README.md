# Configuration

## Broker and topics

| Configuration | Description | Default |
|---------------|-------------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap servers | `localhost:9092` |
| `KAFKA_SPAN_TOPIC` | Topic where incoming spans are stored. | `zipkin-span` |
| `KAFKA_TRACE_TOPIC` | Topic where aggregated traces are stored. | `zipkin-trace` |
| `KAFKA_DEPENDENCY_TOPIC` | Topic where aggregated service dependencies names are stored. | `zipkin-dependency` |

## Storage configurations

| Configuration | Description | Default |
|---------------|-------------|---------|
| `KAFKA_STORAGE_DIR` | Root path where Zipkin stores tracing data | `/data` |
| `KAFKA_STORAGE_TRACE_RETENTION_SCAN_FREQUENCY` | How often to validate traces retention. This will be evaluated at the time events are received. (ms) | `3600000` (1 hour) |
| `KAFKA_STORAGE_TRACE_RETENTION_PERIOD` | How long to keep traces stored. | `604800000` (1 week) |
| `KAFKA_STORAGE_TRACE_INACTIVITY_GAP` | How long to wait until a trace is marked as done (ms). This affects dependency links and traces indexed, but query join traces part to complete results. | `30000` (30 seconds) |
| `KAFKA_STORAGE_DEPENDENCY_RETENTION_PERIOD` | How long to keep dependencies stored. | `604800000` (1 week) |
