#!/bin/bash

FDB_VERSION=7.1.33

docker build \
    -f fdb-dockerfile \
    -t registry.paas/eos-toolchains/go-ycsb:fdb-${FDB_VERSION} \
    --build-arg fdb_version=${FDB_VERSION} \
    .
