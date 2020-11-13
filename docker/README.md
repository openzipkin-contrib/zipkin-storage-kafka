# zipkin-storage-kafka Docker image

To build a zipkin-storage-kafka Docker image, in the top level of the repository, run something
like

## Building

To build a zipkin-storage-kafka Docker image from source, in the top level of the repository, run:


```bash
$ docker/build_image openzipkin-contrib/zipkin-storage-kafka:test
```

To build from a published version, run this instead:

```bash
$ docker/build_image openzipkin-contrib/zipkin-storage-kafka:test 0.18.1
```

