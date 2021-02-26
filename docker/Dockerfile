#
# Copyright 2019-2021 The OpenZipkin Authors
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

# zipkin version should match zipkin.version in /pom.xml
ARG zipkin_version=2.23.2

# java_version is used during the installation process to build or download the module jar.
#
# Use latest version here: https://github.com/orgs/openzipkin/packages/container/package/java
# This is defined in many places because Docker has no "env" script functionality unless you use
# docker-compose: When updating, update everywhere.
ARG java_version=15.0.1_p9

# We copy files from the context into a scratch container first to avoid a problem where docker and
# docker-compose don't share layer hashes https://github.com/docker/compose/issues/883 normally.
# COPY --from= works around the issue.
FROM scratch as scratch

COPY . /code/

# This version is only used during the install process. Try to be consistent as it reduces layers,
# which reduces downloads.
FROM ghcr.io/openzipkin/java:${java_version} as install

WORKDIR /code
# Conditions aren't supported in Dockerfile instructions, so we copy source even if it isn't used.
COPY --from=scratch /code/ .

WORKDIR /install

# When true, build-bin/maven/unjar searches /code for the artifact instead of resolving remotely.
# /code contains what is allowed in .dockerignore. On problem, ensure .dockerignore is correct.
ARG release_from_maven_build=false
ENV RELEASE_FROM_MAVEN_BUILD=$release_from_maven_build
# Version of the artifact to unjar. Ex. "2.4.5" or "2.4.5-SNAPSHOT" "master" to use the pom version.
ARG version=master
ENV VERSION=$version
ENV MAVEN_PROJECT_BASEDIR=/code
RUN /code/build-bin/maven/maven_build_or_unjar io.zipkin.contrib.zipkin-storage-kafka zipkin-module-storage-kafka ${VERSION} module

# zipkin version should match zipkin.version in /code/pom.xml
FROM ghcr.io/openzipkin/zipkin:$zipkin_version as zipkin-storage-kafka
LABEL org.opencontainers.image.description="Zipkin with Kafka Trace storage on OpenJDK and Alpine Linux"
LABEL org.opencontainers.image.source=https://github.com/openzipkin-contrib/zipkin-storage-kafka

# Add installation root as a module
ARG module=storage-kafka
COPY --from=install --chown=${USER} /install/* /zipkin/${module}

# * Active profile typically corresponds $1 in module/src/main/resources/zipkin-server-(.*).yml
ENV MODULE_OPTS="-Dloader.path=${module} -Dspring.profiles.active=${module}"
ENV KAFKA_STORAGE_DIR /zipkin/data
RUN mkdir /zipkin/data

# Install C++
USER root
RUN apk add --update --no-cache libstdc++ libc6-compat && \
    ln -s /lib/libc.musl-x86_64.so.1 /lib/ld-linux-x86-64.so.2
USER ${USER}
