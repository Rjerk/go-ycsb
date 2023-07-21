#!/bin/bash

dynamodbtablename=ycsb
dynamodbprimarykeytype=HASH_AND_RANGE
dynamodbprimarykey=_key
dynamodbhashkey=hashkey
dynamodbhashkeyvalue=hash
dynamodbrcunits=10
dynamodbwcunits=10
dynamodbensurecleantable=false
dynamodbregion=us-east-1
dynamodbconsistentreads=false
dynamodbdeleteafterrunstage=false
dynamodbendpoint=http://100.71.8.128:80

recordcount=50000000
operationcount=50000000
workload=core
fieldcount=5
fieldlength=100

readallfields=true

readproportion=1
updateproportion=0
scanproportion=0
insertproportion=0

requestdistribution=uniform
threads=16
mode=load

image="registry.paas/eos-toolchains/go-ycsb:fdb-7.1.33"

docker run $image $mode dynamodb --threads=$threads \
    -p recordcount=$recordcount \
    -p operationcount=$operationcount \
    -p workload=core \
    -p fieldcount=$fieldcount \
    -p fieldlength=$fieldlength \
    -p readallfields=$readallfields \
    -p readproportion=$readproportion \
    -p updateproportion=$updateproportion \
    -p scanproportion=$scanproportion \
    -p insertproportion=$insertproportion \
    -p requestdistribution=$requestdistribution \
    -p dynamodb.tablename=$dynamodbtablename \
    -p dynamodb.ensure.clean.table=$dynamodbensurecleantable \
    -p dynamodb.primarykey.type=$dynamodbprimarykeytype \
    -p dynamodb.primarykey=$dynamodbprimarykey \
    -p dynamodb.hashkey=$dynamodbhashkey \
    -p dynamodb.hashkey.value=$dynamodbhashkeyvalue \
    -p dynamodb.rc.units=$dynamodbrcunits \
    -p dynamodb.wc.units=$dynamodbwcunits \
    -p dynamodb.region=$dynamodbregion \
    -p dynamodb.consistent.reads=$dynamodbconsistentreads \
    -p dynamodb.delete.after.run.stage=$dynamodbdeleteafterrunstage \
    -p dynamodb.endpoint=$dynamodbendpoint

# fdbcluster=/etc/foundationdb/fdb.cluster
# fdbversion=710
# 
# docker run -v $fdbcluster:$fdbcluster \
#     $image $mode foundationdb \
#     --threads=$threads \
#     -p recordcount=$recordcount \
#     -p operationcount=$operationcount \
#     -p workload=core \
#     -p fieldcount=$fieldcount \
#     -p fieldlength=$fieldlength \
#     -p readallfields=true \
#     -p readproportion=$readproportion \
#     -p updateproportion=$updateproportion \
#     -p scanproportion=$scanproportion \
#     -p insertproportion=$insertproportion \
#     -p requestdistribution=$requestdistribution \
#     -p fdb.cluster=$fdbcluster \
#     -p fdb.version=$fdbversion
# 
