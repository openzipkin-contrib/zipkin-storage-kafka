.PHONY: all
all: build

OPEN := 'xdg-open'
MAVEN := './mvnw'
VERSION := '0.3.1-SNAPSHOT'

.PHONY: run
run: build zipkin-local

.PHONY: run-docker
run-docker: build docker-build docker-up

.PHONY: docker-build
docker-build:
	TAG=${VERSION} \
	docker-compose build

.PHONY: docker-push
docker-push: docker-build
	TAG=${VERSION} \
	docker-compose push

.PHONY: docker-up
docker-up:
	TAG=${VERSION} \
	docker-compose up -d

.PHONY: docker-kafka-up
docker-kafka-up:
	docker-compose up -d kafka zookeeper

.PHONY: license-header
license-header:
	${MAVEN} com.mycila:license-maven-plugin:format

.PHONY: build
build: license-header
	${MAVEN} clean install -DskipTests

.PHONY: test
test: build
	${MAVEN} test verify

.PHONY: zipkin-local
zipkin-local:
	STORAGE_TYPE=kafkastore \
	java \
	-Dloader.path='storage/target/zipkin-storage-kafka-${VERSION}.jar,autoconfigure/target/zipkin-autoconfigure-storage-kafka-${VERSION}-module.jar' \
	-Dspring.profiles.active=kafkastore \
	-cp zipkin.jar \
	org.springframework.boot.loader.PropertiesLauncher

.PHONY: get-zipkin
get-zipkin:
	curl -sSL https://zipkin.io/quickstart.sh | bash -s

.PHONY: zipkin-test
zipkin-test:
	curl -s https://raw.githubusercontent.com/openzipkin/zipkin/master/zipkin-ui/testdata/netflix.json | \
	curl -X POST -s localhost:9411/api/v2/spans -H'Content-Type: application/json' -d @- ; \
	${OPEN} 'http://localhost:9411/zipkin/?lookback=custom&startTs=1'