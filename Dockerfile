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

FROM openjdk:11

ENV ZIPKIN_REPO https://jcenter.bintray.com
ENV ZIPKIN_VERSION 2.12.1

# Use to set heap, trust store or other system properties.
ENV JAVA_OPTS -Djava.security.egd=file:/dev/./urandom
ENV STORAGE_TYPE=kafka
# Add environment settings for supported storage types
WORKDIR /zipkin

RUN curl -SL $ZIPKIN_REPO/io/zipkin/java/zipkin-server/$ZIPKIN_VERSION/zipkin-server-${ZIPKIN_VERSION}-exec.jar > zipkin.jar

ADD storage/target/zipkin-storage-kafka-0.1.0-SNAPSHOT.jar zipkin-storage-kafka.jar
ADD autoconfigure/target/zipkin-autoconfigure-storage-kafka-0.1.0-SNAPSHOT-module.jar zipkin-autoconfigure-storage-kafka.jar


EXPOSE 9410 9411

CMD exec java ${JAVA_OPTS} -Dloader.path='zipkin-storage-kafka.jar,zipkin-autoconfigure-storage-kafka.jar' -Dspring.profiles.active=kafka -cp zipkin.jar org.springframework.boot.loader.PropertiesLauncher