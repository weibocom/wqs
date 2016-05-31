#!/usr/bin/bash
echo "clean ..."
killall labs-sync
rm -rf 1000*.log
echo "clean done"

echo "build ..."
sh build.sh
echo "build done"

echo "start proxy ..."
nohup ./labs-sync 2<&1 > 10002.log &
nohup ./labs-sync -a :10003 -h "http://127.0.0.1:10002/sync;http://127.0.0.1:10004/sync" 2<&1 > 10003.log &
nohup ./labs-sync -a :10004 -h "http://127.0.0.1:10002/sync;http://127.0.0.1:10003/sync" 2<&1 > 10004.log &
sleep 2
echo "proxy is ready"

curl -X POST -d '{"Key":"k1","Val":"v1"}' http://127.0.0.1:10002/set 
curl -X POST -d '{"Key":"k2","Val":"v2"}' http://127.0.0.1:10003/set 
curl -X POST -d '{"Key":"k3","Val":"v3"}' http://127.0.0.1:10004/set 

echo "\n"
grep -E 'set|push|recv' 10002.log
echo "========10002 DONE========"
grep -E 'set|push|recv' 10003.log
echo "========10003 DONE========"
grep -E 'set|push|recv' 10004.log
echo "========10004 DONE========"
