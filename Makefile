# Variables
OPEN := 'xdg-open'
MAVEN := './mvnw'
.PHONY: all
all: build test docker-build-test
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
# Docker tasks
## Build test image
.PHONY: docker-build-test
docker-build-test:
	docker/build_image openzipkincontrib/zipkin-storage-kafka:test
## Run test distributed compose
.PHONY: docker-up-test
docker-up-test:
	docker-compose -f ./.github/workflows/docker/docker-compose.test.yml up -d
## Build local image
.PHONY: docker-build
docker-build:
	docker/build_image openzipkincontrib/zipkin-storage-kafka:latest
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
