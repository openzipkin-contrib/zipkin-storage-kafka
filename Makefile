# Variables
OPEN := 'xdg-open'
MAVEN := './mvnw'
KAFKA_BOOTSTRAP_SERVERS := 'localhost:19092'
VERSION := '0.8.1-SNAPSHOT'
.PHONY: all
all: build test
# Create topics on local kafka
.PHONY: kafka-topics
kafka-topics-local:
	${KAFKA_HOME}/bin/kafka-topics.sh \
		--zookeeper localhost:2181 --create --topic zipkin-spans --partitions 2 --replication-factor 1 --if-not-exists
	${KAFKA_HOME}/bin/kafka-topics.sh \
		--zookeeper localhost:2181 --create --topic zipkin-trace --partitions 2 --replication-factor 1 --if-not-exists
	${KAFKA_HOME}/bin/kafka-topics.sh \
		--zookeeper localhost:2181 --create --topic zipkin-dependency --partitions 2 --replication-factor 1 --if-not-exists
# Maven tasks
## add license headers
.PHONY: license-header
license-header:
	${MAVEN} com.mycila:license-maven-plugin:format
## mvn install
.PHONY: build
build: license-header
	${MAVEN} clean install -DskipTests
## testing
.PHONY: test
test: license-header
	${MAVEN} test verify
# Tasks to run Zipkin locally
## Download zipkin jar
.PHONY: get-zipkin
get-zipkin:
	curl -sSL https://zipkin.io/quickstart.sh | bash -s
## Run built storage with local zipkin (note that it requires a kafka instance running)
.PHONY: zipkin-local
zipkin-local:
	STORAGE_TYPE=kafka \
	KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS} \
	java \
	-Dloader.path='module/target/zipkin-module-storage-kafka-${VERSION}-module.jar,module/target/zipkin-module-storage-kafka-${VERSION}-module.jar!/lib' \
	-Dspring.profiles.active=kafka \
	-cp zipkin.jar \
	org.springframework.boot.loader.PropertiesLauncher
## Task to build and run kafka locally
.PHONY: run-local
run-local: build zipkin-local
# Docker tasks
## Build test image
.PHONY: docker-build-test
docker-build-test:
	docker build -t openzipkincontrib/zipkin-storage-kafka:test -f docker/Dockerfile .
## Run test distributed compose
.PHONY: docker-up-test
docker-up-test:
	docker-compose -f docker/docker-compose.test.yml up -d
## Build local image
.PHONY: docker-build
docker-build:
	docker build -t openzipkincontrib/zipkin-storage-kafka -f docker/Dockerfile .
## Run single instance compose
.PHONY: docker-up-single
docker-up-single:
	docker-compose -f docker/examples/single/docker-compose.yml up -d
## Run single instance compose
.PHONY: docker-up-distributed
docker-up-distributed:
	docker-compose -f docker/examples/distributed/docker-compose.yml up -d
## Task to build and run on docker
.PHONY: run-docker
run-docker: build docker-build docker-up-single
# Testing instances
## Testing distributed instances
.PHONY: zipkin-test-distributed
zipkin-test-distributed:
	curl -s https://raw.githubusercontent.com/openzipkin/zipkin/master/zipkin-lens/testdata/netflix.json | \
	curl -X POST -s localhost:9411/api/v2/spans -H'Content-Type: application/json' -d @- ; \
	${OPEN} 'http://localhost:19411/zipkin/?lookback=custom&startTs=1'
	sleep 61
	curl -s https://raw.githubusercontent.com/openzipkin/zipkin/master/zipkin-lens/testdata/messaging.json | \
	curl -X POST -s localhost:9411/api/v2/spans -H'Content-Type: application/json' -d @- ; \
	${OPEN} 'http://localhost:29411/zipkin/?lookback=custom&startTs=1'
## Testing single instance
.PHONY: zipkin-test-single
zipkin-test-single:
	curl -s https://raw.githubusercontent.com/openzipkin/zipkin/master/zipkin-lens/testdata/netflix.json | \
	curl -X POST -s localhost:9411/api/v2/spans -H'Content-Type: application/json' -d @- ; \
	${OPEN} 'http://localhost:9411/zipkin/?lookback=custom&startTs=1'
	sleep 61
	curl -s https://raw.githubusercontent.com/openzipkin/zipkin/master/zipkin-lens/testdata/messaging.json | \
	curl -X POST -s localhost:9411/api/v2/spans -H'Content-Type: application/json' -d @-
