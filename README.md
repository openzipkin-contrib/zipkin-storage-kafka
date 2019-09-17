# Zipkin Storage: Kafka *[EXPERIMENTAL]*

[![Build Status](https://www.travis-ci.org/openzipkin-contrib/zipkin-storage-kafka.svg?branch=master)](https://www.travis-ci.org/openzipkin-contrib/zipkin-storage-kafka)
[![](https://jitpack.io/v/openzipkin-contrib/zipkin-storage-kafka.svg)](https://jitpack.io/#openzipkin-contrib/zipkin-storage-kafka)
[![](https://images.microbadger.com/badges/version/openzipkin-contrib/zipkin-storage-kafka.svg)](https://microbadger.com/images/openzipkin-contrib/zipkin-storage-kafka "Get your own version badge on microbadger.com")

Kafka-based storage for Zipkin.

```
                    +----------------------------*zipkin*----------------------------------------------
                    |                                     [ dependency-store ]--->( dependencies      )
                    |                                                  ^      +-->( autocomplete-tags )
( collected-spans )-|->[ span-consumer ]  [ aggregation ]    [ trace-store ]--+-->( traces            )
  via http, kafka,  |       |                    ^    |         ^      |      +-->( service-names     )
  amq, grpc, etc.   +-------|--------------------|----|---------|------|-------------------------------
                            |                    |    |         |      |
----------------------------|--------------------|----|---------|------|-------------------------------
                            +-->( spans )--------+----+---------|      |
                                                      |         |      |
*kafka*                                               +->( traces )    |
 topics                                               |                |
                                                      +->( dependencies )
                                                         
-------------------------------------------------------------------------------------------------------

```

> Spans collected via different transports are partitioned by `traceId` and stored in a "spans" Kafka topic.
Partitioned spans are then aggregated into traces and then into dependency links, both 
results are emitted into Kafka topics as well.
These 3 topics are used as source for local stores (Kafka Stream stores) that support Zipkin query and search APIs.

[Design notes](DESIGN.md)

[Configuration](autoconfigure/README.md)

## Building

To build the project you will need Java 8+.

```bash
make build
```

And testing:

```bash
make test
```

If you want to build a docker image:

```bash
make docker-build
```

### Run locally

To run locally, first you need to get Zipkin binaries:

```bash
make get-zipkin
```

By default Zipkin will be waiting for a Kafka broker to be running on `localhost:19092`. If you don't have one, 
this service is available via Docker Compose:

```bash
make docker-kafka-up
```

Then run Zipkin locally:

```bash
make run
```

### Run with Docker

If you have Docker available, run:

```bash
make run-docker 
```

And Docker image will be built and Docker compose will start.

#### Examples

There are two examples, running Zipkin with kafka as storage:

+ [Single-node](docker-compose.yml)
+ [Multi-mode](docker-compose-distributed.yml)

### Testing

To validate storage make sure that Kafka topics are created so Kafka Stream instances can be 
initialized properly:

```bash
make kafka-topics
make zipkin-test
```

This will start a browser and check a traces has been registered.

It will send another trace after a minute (`trace timeout`) + 1 second to trigger
aggregation and visualize dependency graph.

If running multi-node docker example, run:

```bash
make zipkin-test-multi
```

![traces](docs/traces.png)

![dependencies](docs/dependencies.png)

## Acknowledgments

This project is inspired in Adrian Cole's VoltDB storage <https://github.com/adriancole/zipkin-voltdb>

Kafka Streams images are created with <https://zz85.github.io/kafka-streams-viz/>
