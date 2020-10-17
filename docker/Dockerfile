#
# Copyright 2019-2020 The OpenZipkin Authors
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

# We copy files from the context into a scratch container first to avoid a problem where docker and
# docker-compose don't share layer hashes https://github.com/docker/compose/issues/883 normally.
# COPY --from= works around the issue.
FROM scratch as scratch

COPY . /code/

FROM openzipkin/zipkin-builder as built

COPY --from=scratch /code /code

WORKDIR /code
RUN mvn -B --no-transfer-progress package -DskipTests=true -pl module -am

RUN cd /code && mkdir -p /zipkin-storage-kafka && \
    cp module/target/zipkin-module-storage-kafka-*-module.jar /zipkin-storage-kafka && \
    cd /zipkin-storage-kafka && \
    jar xf *.jar && \
    rm *.jar

FROM openzipkin/zipkin:2.21.7

COPY --from=built /zipkin-storage-kafka/ /zipkin/kafka/

RUN echo > .kafka_profile

ENV MODULE_OPTS="-Dloader.path=kafka -Dspring.profiles.active=kafka"
ENV STORAGE_TYPE=kafka
ENV KAFKA_STORAGE_DIR /data

# Prepare state dir
USER root
RUN mkdir /data && chown zipkin:zipkin /data
VOLUME /data
# back to zipkin user
USER zipkin
