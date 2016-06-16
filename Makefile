GO=go
WORKDIR=`pwd`

Branch=`git rev-parse --abbrev-ref HEAD`
SHA1=`git rev-parse --short HEAD`
Date=`date +"%Y-%m-%d"`
Version=$(Branch)@$(SHA1)@$(Date)

default: vet build

dep:
	$(GO) get ./...
	sh script/update_conflict.sh
	cd ./cmd/benchmark
	$(GO) get ./...
	cd $(WORKDIR)
	cd cmd/go_kafka_client_benchmark
	cd $(WORKDIR)
	$(GO) get ./...

vet:
	$(GO) vet ./...

bin:
	@mkdir -p bin

test:
	./script/run_kafka.sh run go test ./...

test-verbose:
	./script/run_kafka.sh run go test ./... -v

test-race:
	./script/run_kafka.sh run go test ./... -v -race

build: build-qservice

build-qservice:
	$(GO) build -ldflags "-X main.version=$(Version)" -o qservice .

benchmark:bin vet
	$(GO) build -o bin/benchmark ./cmd/benchmark/
	$(GO) build -o bin/go_kafka_client_benchmark ./cmd/go_kafka_client_benchmark/

clean:
	@-./script/run_kafka.sh clean
	@rm -rf bin
	@rm -rf qservice
	@echo "clean done"

.PHONY: test testdeps vet clean
