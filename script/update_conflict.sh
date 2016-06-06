#!/bin/sh

prefix=$GOPATH/src/github.com/elodina/go_kafka_client

sed -i "" "137s:k.producer.Close(k.config.ProducerCloseTimeout):k.producer.Close():g" $GOPATH/src/github.com/elodina/go_kafka_client/log_emitters.go

sed -i "" "125s:producer.Close(time.Second):producer.Close():g" $GOPATH/src/github.com/elodina/go_kafka_client/mirror_maker.go

sed -i "" "154s:p.Close(time.Second):p.Close():g" $GOPATH/src/github.com/elodina/go_kafka_client/testing_utils.go

sed -i "" "154s:p.Close(time.Second):p.Close():g" $GOPATH/src/github.com/elodina/go_kafka_client/testing_utils.go

sed -i "" "178s:p.Close(time.Second):p.Close():g" $GOPATH/src/github.com/elodina/go_kafka_client/testing_utils.go

sed -i "" '23s:"time"::g' $GOPATH/src/github.com/elodina/go_kafka_client/mirror_maker.go
