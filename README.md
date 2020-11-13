# Zipkin Storage: Kafka *[EXPERIMENTAL]*

[![Gitter chat](http://img.shields.io/badge/gitter-join%20chat%20%E2%86%92-brightgreen.svg)](https://gitter.im/openzipkin/zipkin)
[![Build Status](https://travis-ci.com/openzipkin-contrib/zipkin-storage-kafka.svg?branch=master)](https://travis-ci.com/openzipkin-contrib/zipkin-storage-kafka)
[![Maven Central](https://img.shields.io/maven-central/v/io.zipkin.gcp/zipkin-module-storage-stackdriver.svg)](https://search.maven.org/search?q=g:io.zipkin.contrib.zipkin-storage-kafka%20AND%20a:zipkin-module-storage-kafka)

Kafka-based storage for Zipkin.

```
                    +----------------------------*zipkin*----------------------------------------------
                    |                                     [ dependency-storage ]--->( dependencies      )
                    |                                                  ^        +-->( autocomplete-tags )
( collected-spans )-|->[ partitioning ]   [ aggregation ]    [ trace-storage ]--+-->( traces            )
  via http, kafka,  |       |                    ^    |         ^      |        +-->( service-names     )
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

> Spans collected via different transports are partitioned by `traceId` and stored in a partitioned spans Kafka topic.
Partitioned spans are then aggregated into traces and then into dependency links, both
results are emitted into Kafka topics as well.
These 3 topics are used as source for local stores (Kafka Stream stores) that support Zipkin query and search APIs.

[Design](storage/README.md)

[Configuration](module/README.md)

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

To try out, there is a [Docker compose](docker/examples/dependencies/docker-compose.yml) configuration ready to test.

If an existing Kafka collector is in place downstreaming traces into an existing storage, another Kafka consumer group id can be used for `zipkin-storage-kafka` to consume traces in parallel. Otherwise, you can [forward spans from another Zipkin server](https://github.com/openzipkin-contrib/zipkin-storage-forwarder)  to `zipkin-storage-kafka` if Kafka transport is not available.

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

By default Zipkin will be waiting for a Kafka broker to be running on `localhost:19092`.

Then run Zipkin locally:

```bash
make run-local
```

To validate storage make sure that Kafka topics are created so Kafka Stream instances can be
initialized properly:

```bash
make kafka-topics
make zipkin-test
```

This will start a browser and check a traces has been registered.

It will send another trace after a minute (`trace timeout`) + 1 second to trigger
aggregation and visualize dependency graph.

### Run with Docker

If you have Docker available, run:

```bash
make run-docker
```

And Docker image will be built and Docker compose will start.

To test it, run:

```bash
make zipkin-test-single
# or
make zipkin-test-distributed
```

![traces](docs/traces.png)

![dependencies](docs/dependencies.png)

### Examples

+ [Single-node](docker/examples/single/docker-compose.yml): span partitioning, aggregation, and storage happening on the same containers.
+ [Distributed-mode](docker/examples/distributed/docker-compose.yml): partitioning and aggregation is in a different container than storage.
+ [Only-dependencies](docker/examples/dependencies/docker-compose.yml): only components to support aggregation and search of dependency graphs.

## Acknowledgments

This project is inspired in Adrian Cole's VoltDB storage <https://github.com/adriancole/zipkin-voltdb>

Kafka Streams images are created with <https://zz85.github.io/kafka-streams-viz/>

## Artifacts
All artifacts publish to the group ID "io.zipkin.contrib.zipkin-storage-kafka". We use a common
release version for all components.

### Library Releases
Releases are at [Sonatype](https://oss.sonatype.org/content/repositories/releases) and [Maven Central](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22io.zipkin.contrib.zipkin-storage-kafka%22)

### Library Snapshots
Snapshots are uploaded to [Sonatype](https://oss.sonatype.org/content/repositories/snapshots) after
commits to master.

### Docker Images
Released versions of zipkin-server are published to Docker Hub as `openzipkincontrib/zipkin-storage-kafka` and GitHub
Container Registry as `ghcr.io/openzipkincontrib/zipkin-storage-kafka`. See [docker](./docker) for details.
