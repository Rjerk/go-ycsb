准备工作
准备表
创建两张表名分别为usertable和usertable1的表

aws dynamodb create-table --table-name usertable --attribute-definitions AttributeName=_key,AttributeType=B --key-schema AttributeName=_key,KeyType=HASH --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 --endpoint http://localhost:3833

aws dynamodb create-table --table-name usertable1 --attribute-definitions AttributeName=_key,AttributeType=B --key-schema AttributeName=_key,KeyType=HASH --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 --endpoint http://localhost:3833
准备配置文件
创建AWSCredentials.properties和dynamodb.properties文件

[root@libo go-ycsb]# pwd

/lb-dev/work/vscode/go-ycsb
[root@libo go-ycsb]# cat dynamodb/conf/AWSCredentials.properties 
accessKey = fakekey
secretKey = fakekey

[root@libo go-ycsb]# cat dynamodb/conf/dynamodb.properties 
dynamodb.awsCredentialsFile = /lb-dev/work/vscode/go-ycsb/dynamodb/conf/AWSCredentials.properties
dynamodb.endpoint = http://192.168.32.11:3833
执行测试
准备数据
此处通过调用TransactWrite接口实现，每个TransactWrite里包含dynamodb.twc.units个Put（同时向usertable和usertable1写）。

workloada

需指定insertproportion=1其他操作为0

# Yahoo! Cloud System Benchmark
# Workload A: Update heavy workload
#   Application example: Session store recording recent actions
#                        
#   Read/update ratio: 50/50
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

recordcount=2
operationcount=2
workload=core

readallfields=true

fieldcount=7
fieldlength=100

readproportion=0
updateproportion=0
scanproportion=0
insertproportion=1

requestdistribution=uniform
执行load

 ./bin/go-ycsb load dynamodb -P workloads/workloada -P dynamodb/conf/dynamodb.properties
进行测试
模拟上传
直接使用workloada即可

调用TransactWrite接口实现，每个TransactWrite里包含1个Put和1个Update（向Usertable执行Update，向usertable1执行Put）。

./bin/go-ycsb run dynamodb -P workloads/workloada -P dynamodb/conf/dynamodb.properties
模拟删除
workloadb

需指定updateproportion=1其他操作为0

# Yahoo! Cloud System Benchmark
# Workload B: Read mostly workload
#   Application example: photo tagging; add a tag is an update, but most operations are to read tags
#                        
#   Read/update ratio: 95/5
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

recordcount=2
operationcount=2
workload=core

readallfields=true

readproportion=0
updateproportion=1
scanproportion=0
insertproportion=0

requestdistribution=uniform


调用TransactWrite接口实现，每个TransactWrite里包含1个Delete和1个Update（向Usertable执行Update，向usertable1执行Delete）。

./bin/go-ycsb run dynamodb -P workloads/workloadb -P dynamodb/conf/dynamodb.properties