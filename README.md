[![codebeat badge](https://codebeat.co/badges/5172eb6e-b7bf-4732-bee6-6e6f46d11538)](https://codebeat.co/projects/github-com-weibocom-wqs)
[![Go Report Card](https://goreportcard.com/badge/github.com/weibocom/wqs)](https://goreportcard.com/report/github.com/weibocom/wqs)

#设计说明：
详见:[设计文档](docs/design_cn.md)

#编译说明：
make

## Running tests
To run tests, call:
```
    $ make test
```
It will download kafka's binary tarball during test cases running if you donot have kafka file in testdata.
If you already have a running kafka in your enviroment, you can export KAFKA_ADDR and ZOOKEEPER_ADDR before 
you exec `make test`.
e.g.
```
	KAFKA_ADDR=localhost:9096 ZOOKEEPER_ADDR=localhost:2181 make test
```
