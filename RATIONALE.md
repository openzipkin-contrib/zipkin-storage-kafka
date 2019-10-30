# zipkin-storage-kafka rationale

## Use-cases

### Replacement for batch-oriented Zipkin dependencies

A limitation of [zipkin-dependencies](https://github.com/openzipkin/zipkin-dependencies) module, is that it requires to be scheduled with a defined frequency. This batch-oriented execution causes out-of-date values until processing runs again.

Kafka-based storage enables aggregating dependencies as spans are received, allowing a (near-)real-time calculation of dependency metrics.

To enable this, other components could be disabled. There is a [profile](module/src/main/resources/zipkin-server-kafka-only-dependencies.yml) prepared to enable aggregation and search of dependency graphs.

This profile can be enable by adding Java option: `-Dspring.profiles.active=kafka-only-dependencies`

Docker image includes a environment variable to set the profile:

```bash
MODULE_OPTS="-Dloader.path=lib -Dspring.profiles.active=kafka-only-dependencies"
```

To try out, there is a [Docker compose](docker/dependencies/docker-compose.yml) configuration ready to test.

If an existing Kafka collector is in place downstreaming traces into an existing storage, another Kafka consumer group id can be used for `zipkin-storage-kafka` to consume traces in parallel. Otherwise, you can [forward spans from another Zipkin server](https://github.com/openzipkin-contrib/zipkin-storage-forwarder)  to `zipkin-storage-kafka` if Kafka transport is not available.