.PHONY: all
all: build

OPEN := 'xdg-open'
MAVEN := './mvnw'

.PHONY: run
run: build zipkin-local

.PHONY: run-docker
run-docker: docker-build docker-up

.PHONY: docker-build
docker-build: build
	docker-compose build

.PHONY: docker-up
docker-up:
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
	STORAGE_TYPE=kafka java -Dloader.path='storage/target/zipkin-storage-kafka-0.1.0-SNAPSHOT.jar,autoconfigure/target/zipkin-autoconfigure-storage-kafka-0.1.0-SNAPSHOT-module.jar' -Dspring.profiles.active=kafka -cp zipkin.jar org.springframework.boot.loader.PropertiesLauncher

.PHONY: get-zipkin
get-zipkin:
	curl -sSL https://zipkin.io/quickstart.sh | bash -s

.PHONY: zipkin-test
zipkin-test:
	curl -s https://raw.githubusercontent.com/openzipkin/zipkin/master/zipkin-ui/testdata/netflix.json | \
	curl -X POST -s localhost:9411/api/v2/spans -H'Content-Type: application/json' -d @- ; \
	${OPEN} 'http://localhost:9411/zipkin/?lookback=custom&startTs=1'