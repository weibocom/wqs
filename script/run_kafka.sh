#!/bin/sh
SCALA_VERSION=2.11
KAFKA_VERSION=0.9.0.1
KAFKA_DIR=kafka_$SCALA_VERSION-$KAFKA_VERSION
KAFKA_SRC=http://www.mirrorservice.org/sites/ftp.apache.org/kafka/$KAFKA_VERSION/$KAFKA_DIR.tgz
KAFKA_ROOT=testdata/$KAFKA_DIR

download_kafka()
{
	[ -d $KAFKA_ROOT ] || {
		mkdir -p $KAFKA_ROOT
		cd $KAFKA_ROOT && curl $KAFKA_SRC | tar xz
	}
}

run_kafka()
{
	mkdir -p /tmp/wqs_testing/{kafka,zookeeper}
	$KAFKA_ROOT/bin/zookeeper-server-start.sh testdata/zookeeper.properties>/dev/null 2>&1 &
	sleep 1
	$KAFKA_ROOT/bin/kafka-server-start.sh testdata/server.properties>/dev/null 2>&1&
}

clean_up()
{
	clean
	sleep 2
	rm -r /tmp/wqs_testing
}

clean()
{
	alias grep='grep'
	ps ax | grep -i 'kafka\.Kafka' | grep java | grep -v grep | awk '{print $1}' | xargs kill -SIGTERM >/dev/null 2>&1
	sleep 1
	ps ax | grep -i 'zookeeper' | grep -v grep | awk '{print $1}' | xargs kill >/dev/null 2>&1
}

running()
{
	#check dependence environment
	[ -z "$KAFKA_ADDR" -o -z "$ZOOKEEPER_ADDR" ] && {
		download_kafka
		run_kafka
		should_cleanup=1
	}

	$*

	# clean up files and processes
	[ "$should_cleanup" = "1" ] && {
		#stop zookeeper and kafka
		clean_up
	}
}

# script start here
should_cleanup=0
RETVAL=0
case "$1" in
	run)
		shift
		running $*
		RETVAL=$?
		;;
	clean)
		clean
		RETVAL=$?
		;;
	*)
		echo $"Usage: $0 {run|clean}"
		RETVAL=2
		;;
esac
exit $RETVAL
