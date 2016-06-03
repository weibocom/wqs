GO=go
SCALA_VERSION?= 2.11
KAFKA_VERSION?= 0.9.0.1
KAFKA_DIR= kafka_$(SCALA_VERSION)-$(KAFKA_VERSION)
KAFKA_SRC= http://www.mirrorservice.org/sites/ftp.apache.org/kafka/$(KAFKA_VERSION)/$(KAFKA_DIR).tgz
KAFKA_ROOT= testdata/$(KAFKA_DIR)
WORKDIR=`pwd`

default: build

dep:
	$(GO) get ./...
	sh hack/update_conflict.sh
	cd ./cmd/benchmark
	$(GO) get ./...
	cd $(WORKDIR)
	cd cmd/go_kafka_client_benchmark
	cd $(WORKDIR)
	$(GO) get ./...

vet:
	go vet ./...

$(KAFKA_ROOT):
	@mkdir -p $(dir $@)
	cd $(dir $@) && curl $(KAFKA_SRC) | tar xz

testdeps: $(KAFKA_ROOT)

bin:
	@mkdir -p bin

test: testdeps
	KAFKA_DIR=$(KAFKA_DIR) go test ./...

test-verbose: testdeps
	KAFKA_DIR=$(KAFKA_DIR) go test ./... -v

test-race: testdeps
	KAFKA_DIR=$(KAFKA_DIR) go test ./... -v -race

build: build-qservice

build-qservice:
	$(GO) build -o qservice .


benchmark:bin
	$(GO) build -o bin/benchmark ./cmd/benchmark/
	$(GO) build -o bin/go_kafka_client_benchmark ./cmd/go_kafka_client_benchmark/

clean:
	@rm -rf bin
	@rm -rf qservice
	@echo "clean done"

.PHONY: test testdeps vet clean
