#
# Copyright 2019 jeqo
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
# in compliance with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under
# the License.
#

FROM alpine

ENV ZIPKIN_REPO https://repo1.maven.org/maven2
ENV ZIPKIN_VERSION 2.16.0

# Add environment settings for supported storage types
COPY docker/ /zipkin/

WORKDIR /zipkin


RUN apk add unzip curl --no-cache && \
    curl -SL $ZIPKIN_REPO/io/zipkin/zipkin-server/$ZIPKIN_VERSION/zipkin-server-$ZIPKIN_VERSION-exec.jar > zipkin-server.jar && \
    # don't break when unzip finds an extra header https://github.com/openzipkin/zipkin/issues/1932
    unzip zipkin-server.jar ; \
    rm zipkin-server.jar && \
    apk del unzip

FROM gcr.io/distroless/java:11-debug

ARG KAFKA_STORAGE_VERSION=0.4.1-SNAPSHOT
ENV STORAGE_TYPE KAFKASTORE
# Use to set heap, trust store or other system properties.
ENV JAVA_OPTS -Djava.security.egd=file:/dev/./urandom
# 3rd party modules like zipkin-aws will apply profile settings with this
ENV MODULE_OPTS -Dloader.path='zipkin-autoconfigure-storage-kafka.jar,zipkin-autoconfigure-storage-kafka.jar!/lib' \
                -Dspring.profiles.active=kafkastore

RUN ["/busybox/sh", "-c", "adduser -g '' -D zipkin"]

# Add environment settings for supported storage types
COPY --from=0 /zipkin/ /zipkin/
WORKDIR /zipkin

ADD storage/target/zipkin-storage-kafka-${KAFKA_STORAGE_VERSION}.jar zipkin-storage-kafka.jar
ADD autoconfigure/target/zipkin-autoconfigure-storage-kafka-${KAFKA_STORAGE_VERSION}-module.jar zipkin-autoconfigure-storage-kafka.jar

RUN ["/busybox/sh", "-c", "ln -s /busybox/* /bin"]

USER zipkin

EXPOSE 9410 9411

ENTRYPOINT ["/busybox/sh", "run.sh"]
