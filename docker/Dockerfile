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

# This version is only used during the install process. Try to be consistent as it reduces layers,
# which reduces downloads.
FROM ghcr.io/openzipkin/java:15.0.1_p9 as install

WORKDIR /code
# Conditions aren't supported in Dockerfile instructions, so we copy source even if it isn't used.
COPY --from=scratch /code/ .

# Ex. "2.4.5" or "2.4.5-SNAPSHOT" Empty to use the pom version.
ARG version=
ENV VERSION=$version
RUN if [ -n "${VERSION}" ]; then echo "${VERSION}" > version; else \
      sed -n 's/.*<version>\(.*\)<\/version>.*/\1/p' pom.xml| head -1 > version; \
    fi

# Download or build the project's main artifact, then unzip it into /install
ENV ARTIFACT_ID=zipkin-module-storage-kafka
RUN ./build-bin/maven/maven_build_or_unjar io.zipkin.contrib.zipkin-storage-kafka ${ARTIFACT_ID} $(cat version) module && \
    mv $ARTIFACT_ID /install

# zipkin version should match zipkin.version in /code/pom.xml
FROM ghcr.io/openzipkin/zipkin:2.22.2 as zipkin-storage-kafka
LABEL org.opencontainers.image.description="Zipkin with Kafka Trace storage on OpenJDK and Alpine Linux"
LABEL org.opencontainers.image.source=https://github.com/openzipkin-contrib/zipkin-storage-kafka

# Add installation root as a module
ARG module=storage-kafka
COPY --from=install --chown=${USER} /install/ /zipkin/${module}/

# * Active profile typically corresponds $1 in module/src/main/resources/zipkin-server-(.*).yml
ENV MODULE_OPTS="-Dloader.path=${module} -Dspring.profiles.active=${module}"
ENV KAFKA_STORAGE_DIR /zipkin/data
RUN mkdir /zipkin/data
