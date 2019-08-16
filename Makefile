.PHONY: all
all: build

OPEN := 'xdg-open'
MAVEN := './mvnw'
VERSION := '0.4.1-SNAPSHOT'
IMAGE_NAME := 'jeqo/zipkin-kafka'

.PHONY: run
run: build zipkin-local

.PHONY: run-docker
run-docker: build docker-build docker-up

.PHONY: kafka-topics
kafka-topics:
	docker-compose exec kafka-zookeeper /busybox/sh /kafka/bin/kafka-run-class.sh kafka.admin.TopicCommand --zookeeper localhost:2181 --create --topic zipkin-spans --partitions 1 --replication-factor 1 --if-not-exists
	docker-compose exec kafka-zookeeper /busybox/sh /kafka/bin/kafka-run-class.sh kafka.admin.TopicCommand --zookeeper localhost:2181 --create --topic zipkin-traces --partitions 1 --replication-factor 1 --if-not-exists
	docker-compose exec kafka-zookeeper /busybox/sh /kafka/bin/kafka-run-class.sh kafka.admin.TopicCommand --zookeeper localhost:2181 --create --topic zipkin-dependencies --partitions 1 --replication-factor 1 --if-not-exists

.PHONY: docker-build
docker-build:
	docker build -t ${IMAGE_NAME}:latest .
	docker build -t ${IMAGE_NAME}:${VERSION} .

.PHONY: docker-push
docker-push: docker-build
	docker push ${IMAGE_NAME}:latest
	docker push ${IMAGE_NAME}:${VERSION}

.PHONY: docker-up
docker-up:
	TAG=${VERSION} \
	docker-compose up -d

.PHONY: docker-down
docker-down:
	TAG=${VERSION} \
	docker-compose down --remove-orphans

.PHONY: docker-kafka-up
docker-kafka-up:
	docker-compose up -d kafka-zookeeper

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
	KAFKA_BOOTSTRAP_SERVERS=localhost:19092 \
	KAFKA_STORE_BOOTSTRAP_SERVERS=localhost:19092 \
	java \
	-Dloader.path='autoconfigure/target/zipkin-autoconfigure-storage-kafka-${VERSION}-module.jar,autoconfigure/target/zipkin-autoconfigure-storage-kafka-${VERSION}-module.jar!/lib' \
	-Dspring.profiles.active=kafkastore \
	-cp zipkin.jar \
	org.springframework.boot.loader.PropertiesLauncher

.PHONY: get-zipkin
get-zipkin:
	curl -sSL https://zipkin.io/quickstart.sh | bash -s

.PHONY: zipkin-test-multi
zipkin-test-multi:
	curl -s https://raw.githubusercontent.com/openzipkin/zipkin/master/zipkin-lens/testdata/messaging.json | \
	curl -X POST -s localhost:9411/api/v2/spans -H'Content-Type: application/json' -d @- ; \
	${OPEN} 'http://localhost:9412/zipkin/?lookback=custom&startTs=1'
	echo 'waiting for a minute to send another span and trigger aggregation'
	sleep 6
	curl -s https://raw.githubusercontent.com/openzipkin/zipkin/master/zipkin-lens/testdata/netflix.json | \
	curl -X POST -s localhost:9411/api/v2/spans -H'Content-Type: application/json' -d @- ; \
	sleep 6
	curl -s https://raw.githubusercontent.com/openzipkin/zipkin/master/zipkin-lens/testdata/skew.json | \
	curl -X POST -s localhost:9411/api/v2/spans -H'Content-Type: application/json' -d @- ; \

.PHONY: zipkin-test
zipkin-test:
	curl -s https://raw.githubusercontent.com/openzipkin/zipkin/master/zipkin-lens/testdata/messaging.json | \
	curl -X POST -s localhost:9411/api/v2/spans -H'Content-Type: application/json' -d @- ; \
	${OPEN} 'http://localhost:9411/zipkin/?lookback=custom&startTs=1'
	echo 'waiting for a minute to send another span and trigger aggregation'
	sleep 10
	curl -s https://raw.githubusercontent.com/openzipkin/zipkin/master/zipkin-lens/testdata/netflix.json | \
	curl -X POST -s localhost:9411/api/v2/spans -H'Content-Type: application/json' -d @- ; \
	sleep 10
	curl -s https://raw.githubusercontent.com/openzipkin/zipkin/master/zipkin-lens/testdata/skew.json | \
	curl -X POST -s localhost:9411/api/v2/spans -H'Content-Type: application/json' -d @- ; \

.PHONY: release
release:
	${MAVEN} release:prepare
	${MAVEN} release:perform
