.PHONY: all
all: build

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
	./mvnw com.mycila:license-maven-plugin:format

.PHONY: build
build: license-header
	./mvnw clean install -DskipTests

.PHONY: zipkin-local
zipkin-local:
	STORAGE_TYPE=kafka java -Dloader.path='storage/target/zipkin-storage-kafka-0.1.0-SNAPSHOT.jar,autoconfigure/target/zipkin-autoconfigure-storage-kafka-0.1.0-SNAPSHOT-module.jar' -Dspring.profiles.active=kafka -cp zipkin.jar org.springframework.boot.loader.PropertiesLauncher
#--zipkin.ui.source-root=classpath:zipkin-lens

.PHONY: get-zipkin
get-zipkin:
	curl -sSL https://zipkin.io/quickstart.sh | bash -s