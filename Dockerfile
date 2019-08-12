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
ENV KAFKASTORE_VERSION 0.4.1-SNAPSHOT

WORKDIR /zipkin

RUN apk add unzip curl --no-cache && \
    curl -SL $ZIPKIN_REPO/io/zipkin/zipkin-server/$ZIPKIN_VERSION/zipkin-server-$ZIPKIN_VERSION-exec.jar > zipkin-server.jar && \
    # don't break when unzip finds an extra header https://github.com/openzipkin/zipkin/issues/1932
    unzip zipkin-server.jar ; \
    rm zipkin-server.jar
    # && \
    # apk del unzip

COPY autoconfigure/target/zipkin-autoconfigure-storage-kafka-${KAFKASTORE_VERSION}-module.jar BOOT-INF/lib/kafkastore-module.jar
RUN unzip -o BOOT-INF/lib/kafkastore-module.jar lib/* -d BOOT-INF

FROM gcr.io/distroless/java:11-debug

# Use to set heap, trust store or other system properties.
ENV JAVA_OPTS -Djava.security.egd=file:/dev/./urandom

RUN ["/busybox/sh", "-c", "adduser -g '' -D zipkin"]

# Add environment settings for supported storage types
ENV STORAGE_TYPE kafkastore

COPY --from=0 /zipkin/ /zipkin/
WORKDIR /zipkin

#COPY autoconfigure/target/zipkin-autoconfigure-storage-kafka-${KAFKASTORE_VERSION}-module.jar kafkastore-module.jar

#ENV MODULE_OPTS -Dloader.path='BOOT-INF/lib/kafkastore-module.jar,BOOT-INF/lib/kafkastore-module.jar!/lib' -Dspring.profiles.active=kafkastore
ENV MODULE_OPTS -Dspring.profiles.active=kafkastore

RUN ["/busybox/sh", "-c", "ln -s /busybox/* /bin"]

RUN mkdir /data  && chown zipkin /data
VOLUME /data

USER zipkin

EXPOSE 9411

ENTRYPOINT ["/busybox/sh", "-c", "exec java ${MODULE_OPTS} ${JAVA_OPTS} -cp . org.springframework.boot.loader.PropertiesLauncher"]
