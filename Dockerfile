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

ENV USER jeqo
ENV VERSION 0.5.1

WORKDIR /zipkin

RUN apk add curl unzip && \
  curl -SL https://jitpack.io/com/github/${USER}/zipkin-storage-kafka/zipkin-autoconfigure-storage-kafka/${VERSION}/zipkin-autoconfigure-storage-kafka-${VERSION}-module.jar > kafka.jar && \
  echo > .kafka_profile && \
  unzip kafka.jar -d kafka && \
  rm kafka.jar

FROM openzipkin/zipkin:2.16.2

COPY --from=0 /zipkin/ /zipkin/

ENV MODULE_OPTS="-Dloader.path=kafka -Dspring.profiles.active=kafka"

ENV KAFKA_STORAGE_DIR /data
VOLUME /data
