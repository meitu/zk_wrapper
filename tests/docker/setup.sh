#!/bin/sh
docker-compose -p go_zookeeper_wrapper_test up --build -d
docker-compose -p go_zookeeper_wrapper_test exec -T zookeeper sh -c 'until nc -z 127.0.0.1 2181; do echo "zk is not ready"; sleep 1; done'
