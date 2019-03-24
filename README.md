# Zipkin Storage: Kafka

[![Build Status](https://www.travis-ci.org/jeqo/zipkin-storage-kafka.svg?branch=master)](https://www.travis-ci.org/jeqo/zipkin-storage-kafka)

Kafka-based storage for Zipkin.

> This is in experimentation phase at the moment.

```
                    +----------------------------*zipkin*-------------------------------------------
                    |                                                           +-->( service:span )
                    |                                                           +-->( span-index   )
( collected-spans )-|->[ span-consumer ]        [ aggregation ] [ span-store ]--+-->( traces       )
                    |       |                        ^    |         ^           +-->( dependencies )
                    +-------|------------------------|----|---------|-------------------------------
                            |                        |    |         |     
----------------------------|------------------------|----|---------|-------------------------
                            |                        |    |         |     
                            |                        |    |         |     
*kafka*                     +-->( trace-spans  )---// enriching //--+------->( service:span )
                            |                     // sampling  //   |     
                            +-->( service-span )-// filtering // ---+------->( dependencies )
                                                     |    ^
-----------------------------------------------------|----|---------------------------------
                                                     |    |
*stream-processors*                           [ custom processors ]--->( other storages )



```

- [Design notes](DESIGN.md)

## Configuration

### Storage configurations

| Configuration | Description | Default |
|---------------|-------------|---------|
| `KAFKA_STORE_SPAN_CONSUMER_ENABLED` | Process spans collected by Zipkin server | `true` |
| `KAFKA_STORE_SPAN_STORE_ENABLED` | Aggregate and store Zipkin data | `true` |
| `KAFKA_STORE_BOOTSTRAP_SERVERS` | Kafka bootstrap servers, format: `host:port` | `localhost:9092` |
| `KAFKA_STORE_ENSURE_TOPICS` | Ensure topics are created if don't exist | `true` |
| `KAFKA_STORE_DIRECTORY` | Root path where Zipkin stores tracing data | `/tmp/zipkin` |
| `KAFKA_STORE_COMPRESSION_TYPE` | Compression type used to store data in Kafka topics | `NONE` |
| `KAFKA_STORE_RETENTION_SCAN_FREQUENCY` | Frequency to scan old records, in milliseconds. | `86400000` (1 day) |
| `KAFKA_STORE_RETENTION_MAX_AGE` | Max age of a trace, to recognize old one for retention policies. | `604800000` (7 day) |

### Topics configuration

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

## Get started

To build the project you will need Java 8+.

```bash
make build
make test
```

### Run locally

To run locally, first you need to get Zipkin binaries:

```bash
make get-zipkin
```

By default Zipkin will be waiting for a Kafka broker to be running on `localhost:29092`. If you don't have one, 
this service is available via Docker Compose:

```bash
make docker-kafka-up
```

Then run Zipkin locally:

```bash
make run
```

### Run with Docker

Run:

```bash
make run-docker
```

And Docker image will be built and Docker compose will start.

### Testing

To validate storage:

```bash
make zipkin-test
```

This will start a browser and check a traces has been registered.

### Examples

There are two examples, running zipkin with kafka as storage:

+ Single-node: `examples/single-node`
+ Multi-mode: `examples/multi-mode`

## Acknowledged

This project is inspired in Adrian Cole's <https://github.com/adriancole/zipkin-voltdb>
