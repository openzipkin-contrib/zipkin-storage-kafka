# Configuration

## Broker and topics

| Configuration | Description | Default |
|---------------|-------------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap servers | `localhost:9092` |
| `KAFKA_SPANS_TOPIC` | Topic where incoming list of spans are stored. | `zipkin-spans` |
| `KAFKA_TRACE_TOPIC` | Topic where aggregated traces are stored. | `zipkin-trace` |
| `KAFKA_DEPENDENCY_TOPIC` | Topic where aggregated service dependencies names are stored. | `zipkin-dependency` |

> These topics can be configured by individual components (span-consumer, aggregation, storage) by using Java options. 
> For more information about property names, check [zipkin-server-kafka.yml](src/main/resources/zipkin-server-kafka.yml)

## Storage configurations

| Configuration | Description | Default |
|---------------|-------------|---------|
| `KAFKA_STORAGE_HOST_NAME` | Host name used by storage instances to scatter-gather results | `localhost` |
| `KAFKA_STORAGE_DIR` | Root path where Zipkin stores tracing data | `/tmp/zipkin-storage-kafka` |
| `KAFKA_STORAGE_PARTITIONING_ENABLED` | Flag to enable [Span partitioning](../storage/README.md#span-consumer). | `true` |
| `KAFKA_STORAGE_AGGREGATION_ENABLED` | Flag to enable [Span aggregation](../storage/README.md#span-aggregation). | `true` |
| `KAFKA_STORAGE_AGGREGATION_TRACE_TIMEOUT` | How long to wait until a trace window is closed (ms). If this config is to small, dependency links won't be caught and metrics may drift. | `600000` (1 minute) |
| `KAFKA_STORAGE_TRACE_ENABLED` | Flag to enable [Trace storage](../storage/README.md#trace-storage). | `true` |
| `KAFKA_STORAGE_TRACE_TTL` | How long to keep traces stored. | `259200000` (3 days) |
| `KAFKA_STORAGE_TRACE_TTL_CHECK_INTERVAL` | How often check traces stored TTL. | `3600000` (1 hour) |
| `KAFKA_STORAGE_DEPENDENCY_ENABLED` | Flag to enable [Dependency storage](../storage/README.md#dependency-storage). | `true` |
| `KAFKA_STORAGE_DEPENDENCY_TTL` | How long to keep dependencies stored. | `604800000` (1 week) |

### When running on Kubernetes/Openshift

When running on Kubernetes/Openshift is recommended to use `statefulsets` in order to maintain
storage state directories.

#### Configure hostname

For instances to access other pods on the stateful set, we have to use valid DNS-names:

```yaml
          env:
            - name: STORAGE_TYPE
              value: kafka
            # Gather hostname (name-${POD_ID}) from metadata
            - name: HOSTNAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            # Mapping hostname to Kubernetes DNS defined service name (${NAME}-${POD_ID}.${SVC}.${NAMESPACE}.svc.cluster.local),
            # then instance storage becomes accessible between them
            - name: KAFKA_STORAGE_HOST_NAME
              value: $(HOSTNAME).zipkin.default.svc.cluster.local
```
