# Configuration

## Storage configurations

| Configuration | Description | Default |
|---------------|-------------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap servers | `localhost:9092` |
| `KAFKA_STORAGE_DIR` | Root path where Zipkin stores tracing data | `/data` |
| `KAFKA_SPANS_TOPIC` | Topic where incoming spans are stored. | `zipkin-spans` |
| `KAFKA_TRACES_TOPIC` | Topic where aggregated traces are stored. | `zipkin-traces` |
| `KAFKA_DEPENDENCIES_TOPIC` | Topic where aggregated service dependencies names are stored. | `zipkin-dependencies` |

## Storage durations and timeouts

| Configuration | Description | Default |
|---------------|-------------|---------|
| `KAFKA_STORAGE_TRACES_RETENTION_SCAN_FREQUENCY` | How often to validate traces retention. This will be evaluated at the time events are received. (ms) | `3600000` (1 hour) |
| `KAFKA_STORAGE_TRACES_RETENTION_PERIOD` | How long to keep traces stored. | `604800000` (1 week) |
| `KAFKA_STORAGE_TRACES_INACTIVITY_GAP` | How long to wait until a trace is marked as done (ms). This affects dependency links and traces indexed, but query join traces part to complete results. | `30000` (30 seconds) |
| `KAFKA_STORAGE_DEPENDENCIES_RETENTION_PERIOD` | How long to keep dependencies stored. | `604800000` (1 week) |
