GO=go
WORKDIR=`pwd`

all: build

dep:
	$(GO) get ./...
	sh hack/update_conflict.sh
	cd ./cmd/benchmark
	$(GO) get ./...
	cd $(WORKDIR)
	cd cmd/go_kafka_client_benchmark
	cd $(WORKDIR)
	$(GO) get ./...

lic:
	sh hack/add_licence.sh

build: build-qservice

build-qservice:
	$(GO) build -o qservice .

benchmark:
	@mkdir -p bin
	$(GO) build -o bin/benchmark ./cmd/benchmark/
	$(GO) build -o bin/go_kafka_client_benchmark ./cmd/go_kafka_client_benchmark/

clean:
	@rm -rf bin
	@rm -rf qservice
