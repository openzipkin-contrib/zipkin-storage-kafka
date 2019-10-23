# zipkin-storage-kafka rationale

## Use-cases

### Replacement for Spark-based Zipkin dependencies

One limitation of [zipkin-dependencies](https://github.com/openzipkin/zipkin-dependencies) module based on Spark is that it requires to be scheduled with a defined frequency. This batch-oriented execution causes out-of-date values until processing runs again.

A Kafka-based storage brings the opportunity to aggregate dependencies as spans are received, allowing a (near-)real-time calculation of dependency metrics.

To enable this, other components could be disabled:

```yaml
zipkin:
  storage:
    kafka:
      # ...
      # Kafka Storage flags
      span-partitioning-enabled: true # enable partition of spans by trace-id
      span-aggregation-enabled: true # aggregate spans into traces, and traces into dependency links
      trace-by-id-query-enabled: false 
      trace-search-enabled: false
      dependency-query-enabled: true # or `false` if you want to downstream to another storage
```

By environment variables:

```bash
JAVA_OPTS="-Dzipkin.storage.kafka.trace-by-id-query-enabled=false -Dzipkin.storage.kafka.trace-search-enabled=false"
```

If Kafka collector is in place, `zipkin-storage-kafka` can be run in parallel with another group id. Otherwise, you can [forward spans from another Zipkin server](https://github.com/openzipkin-contrib/zipkin-storage-forwarder) to `zipkin-storage-kafka`.