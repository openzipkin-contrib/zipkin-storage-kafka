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
ENV ZIPKIN_VERSION 2.16.2

WORKDIR /zipkin

RUN apk add unzip curl --no-cache && \
    curl -SL $ZIPKIN_REPO/io/zipkin/zipkin-server/$ZIPKIN_VERSION/zipkin-server-$ZIPKIN_VERSION-exec.jar > zipkin-server.jar

FROM gcr.io/distroless/java:11-debug

# Use to set heap, trust store or other system properties.
ENV JAVA_OPTS -Djava.security.egd=file:/dev/./urandom

RUN ["/busybox/sh", "-c", "adduser -g '' -D zipkin"]

# Add environment settings for supported storage types
ENV KAFKA_STORAGE_VERSION 0.5.1-SNAPSHOT
ENV STORAGE_TYPE kafka

COPY --from=0 /zipkin/ /zipkin/
WORKDIR /zipkin

COPY autoconfigure/target/zipkin-autoconfigure-storage-kafka-${KAFKA_STORAGE_VERSION}-module.jar kafka-module.jar
ENV MODULE_OPTS -Dloader.path='kafka-module.jar,kafka-module.jar!/lib' -Dspring.profiles.active=kafka

RUN ["/busybox/sh", "-c", "ln -s /busybox/* /bin"]

ENV KAFKA_STORAGE_DIR /data
RUN mkdir /data  && chown zipkin /data
VOLUME /data

USER zipkin

EXPOSE 9411

ENTRYPOINT ["/busybox/sh", "-c", "exec java ${MODULE_OPTS} ${JAVA_OPTS} -cp zipkin-server.jar org.springframework.boot.loader.PropertiesLauncher"]
