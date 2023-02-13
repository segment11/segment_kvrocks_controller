# segment kvrocks controller

This project is a command line tool as a controller for [Apache Kvrocks](https://kvrocks.apache.org/). It also supports Redis cluster.

## Features

- meet node
- forget node
- import from exist kvrocks/redis cluster
- migrate slots, support given any slots from one node to another
- balance slots range
- tmp save migrated slots when node crash down and continue after restart
- failover
- check cluster nodes state/nodes number/slot range

## Run Steps

### Run in docker

```shell script
- docker run -it --name=segment_kvrocks_controller --net=host -v /opt/data:/opt/data key232323/segment_kvrocks_controller:latest bash
- java -jar segment_kvrocks_controller-1.0.jar dbDataDir=/opt/data/kvrocks_controller_data
```

### Run in jar

```shell script
- java -jar segment_kvrocks_controller-1.0.jar app.engine.isRedis=0
```

### Compile from sources or Debug in IDE(eg. IDEA) 

TIPS: Your need install jdk8+/gradle6.x+

- mkdir /opt/your-dir
- cd /opt/your-dir
- git clone git@github.com:segment11/segment-leaf.git segment-leaf
- git clone git@github.com:segment11/segmentd.git segmentd
- git clone git@github.com:segment11/segment_kvrocks_controller.git segment_kvrocks_controller
- cd segment_kvrocks_controller
- gradle jar
- cd build/libs
- java -jar segment_kvrocks_controller-1.0.jar

## Configuration

This project has a default conf.properties in jar. You can overwrite configure item values when start.

```shell script
java -jar segment_kvrocks_controller-1.0.jar app.engine.isRedis=1 dbDataDir=/your-dir/kvrocks_controller_data
```

Tips: Item dbDataDir is a H2 database local file.

```
dbDataDir=D:/opt/kvrocks_controller_data

app.engine.isRedis=0
isLocalTest=0

job.migrate.wait.once.ms=10
# 2min
job.migrate.wait.max=12000
job.offset.check.wait.once.ms=100
# 5min
job.offset.check.wait.max=3000
# for redis
job.migrate.keys.once.number=100

jedis.pool.maxTotal=10
jedis.pool.maxIdle=5
jedis.pool.maxWait.ms=5000
jedis.read.timeout.ms=10000

#app.globalName=test
#app.globalPassword=test1234

#cli.runtime.jar=1
```

## Command line examples

TIPS:

--name/--password/--ip/--port will be cached in the current session, so you can omit them.

Import from a cluster:

```shell script
--import=192.168.99.100:6379 --name=TestCluster --password=***
```

Add a primary shard node:

```shell script
-a -m -s=0 --ip=192.168.99.100 --port=6379 --name=TestCluster --password=***
-a -m -s=1 --ip=192.168.99.100 --port=6380
-a -m -s=2 --ip=192.168.99.100 --port=6381
```

Add a replica shard node:

```shell script
-a -s=0 -r=0 --ip=192.168.99.100 --port=6479
```

Delete one shard node:

```shell script
-d --ip=192.168.99.100 --port=6479
```

Migrate some slots to target shard node:

```shell script
-F=0-100,100,200 --ip=192.168.99.100 --port=6380
```

Balance all slots range avg to one shard primary node:

```shell script
-S=192.168.99.100:6380
```

Failover one shard to target replica shard node:

```shell script
-f -s=0 -r=0
```

Fix one shard node when crash down, after restart kvrocks process: 

```shell script
-M --ip=192.168.99.100 --port=6380
```

Clear local H2 database records, then you can import or add shard node from beginning:

```shell script
--clear -V
# or
-D
```

View the current session variables:

```shell script
-X
```

View the current cluster all shard nodes slots range:

```shell script
-V
# view only one shard node
-v --ip=192.168.99.100 --port=6380
```


Check the current cluster all shard nodes state:

```shell script
-A
# check only one shard node
-c --ip=192.168.99.100 --port=6380
```

View the current cluster jobs:

```shell script
-J
# redo one job
-R=jobId1
# redo one job force
-R=jobId1 -O
# delete target job records
-L=jobId1,jobId2
# delete all job records
-L=*
# view tmp saved migrated slots record list in history jobs (eg. see more detail for migrate slots using -F=0-10 --ip=** --port=**)
-U
# delete tmp saved migrated slots record in one job (usually for crash slot range total overlay situation)
-T=jobId1,jobId2
-T=*
```

Others:

```shell script
# help
-H
# quit
-Q
# set target shard node kvrocks process status running (usually for in maintain or test)
-Y --ip=192.168.99.100 --port=6380
# set target shard node kvrocks process status stopped (usually for in maintain or test)
-N --ip=192.168.99.100 --port=6380
```
