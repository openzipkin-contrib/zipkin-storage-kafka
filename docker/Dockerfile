#
# Copyright 2019 The OpenZipkin Authors
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

FROM openzipkin/zipkin-builder

WORKDIR /code
COPY . /code
RUN mvn -B --no-transfer-progress package -DskipTests=true -pl module -am

RUN cd /code && mkdir -p /zipkin-storage-kafka && \
    cp module/target/zipkin-module-storage-kafka-*-module.jar /zipkin-storage-kafka && \
    cd /zipkin-storage-kafka && \
    jar xf *.jar && \
    rm *.jar

FROM openzipkin/zipkin:2.19.1
MAINTAINER Zipkin "https://zipkin.io/"

COPY --from=0 /zipkin-storage-kafka/ /zipkin/lib/

RUN echo > .kafka_profile

ENV MODULE_OPTS="-Dloader.path=lib -Dspring.profiles.active=kafka"
ENV STORAGE_TYPE=kafka
ENV KAFKA_STORAGE_DIR /data

# Prepare state dir
USER root
RUN mkdir /data && chown zipkin:zipkin /data
VOLUME /data
# back to zipkin user
USER zipkin
