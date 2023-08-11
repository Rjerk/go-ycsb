#!/bin/bash
all_num=5
for num in `seq 1 ${all_num}`
do {
    ./go-ycsb run dynamodb -P workload${num} -P dynamodb.properties --threads 8 > ./run${num}.log
} &
done
