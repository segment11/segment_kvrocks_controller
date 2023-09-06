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
docker run -it --name=segment_kvrocks_controller --net=host -v /opt/data:/opt/data key232323/segment_kvrocks_controller:latest bash
java -jar segment_kvrocks_controller-1.0.jar dbDataFile=/opt/data/kvrocks_controller_data
```

### Run in jar

TIPS: This is for kvrocks cluster, set app.engine.isRedis=1 for redis cluster.

```shell script
java -jar segment_kvrocks_controller-1.0.jar app.engine.isRedis=0
```

### Compile from sources or Debug in IDE(eg. IDEA) 

TIPS: Your need install jdk8+/gradle6.x+

- mkdir /opt/your-dir
- cd /opt/your-dir
- git clone git@github.com:segment11/segment_kvrocks_controller.git segment_kvrocks_controller
- cd segment_kvrocks_controller
- gradle jar
- cd build/libs
- java -jar segment_kvrocks_controller-1.0.jar

## Configuration

This project has a default conf.properties in jar. You can overwrite configure item values when start.

```shell script
java -jar segment_kvrocks_controller-1.0.jar app.engine.isRedis=0 dbDataFile=/your-dir/kvrocks_controller_data
```

Tips: Item dbDataFile is a H2 database local file.

```
dbDataFile=D:/opt/kvrocks_controller_data

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

Usage:

```shell script
usage: please input follow args to run task
 -A,--check_all_shard_node                       check all shard nodes
                                                 cluster info/nodes/slots
                                                 if ok and match
 -a,--meet_node                                  operation: add one shard
                                                 node to cluster
 -b,--init_batch <arg>                           operation: add some
                                                 primary shard nodes to
                                                 cluster from beginning,
                                                 eg.
                                                 -b=192.168.99.100:6379,19
                                                 2.168.99.100:6380
 -C,--clear                                      clear old application
                                                 saved local
 -c,--check_shard_node                           check one shard node
                                                 cluster info/nodes/slots
                                                 if ok and match
 -d,--forget_node                                operation: remove one
                                                 shard node from cluster
 -D,--delete                                     delete application local
 -f,--failover                                   operation: failover one
                                                 shard replica node as
                                                 primary
 -F,--migrate_slots <arg>                        migrate some slots to
                                                 current session ip/port
                                                 shard node, eg. -F=0
 -h,--ip <arg>                                   target node ip
 -H,--help                                       args help
 -I,--import <arg>                               import new application
                                                 from exist kvrocks
                                                 cluster, require ip:port,
                                                 eg.
                                                 -I=192.168.99.100:6379
 -J,--job_log                                    display job log
 -L,--delete_job_log <arg>                       delete job log by id, eg.
                                                 -L=* or -L=1,2,3
 -m,--is_primary                                 is primary, eg. true or
                                                 false
 -M,--fix_migrating_node                         fix refresh cluster nodes
                                                 after restart when
                                                 migrate job undone
                                                 @deprecated
 -n,--name <arg>                                 define a unique
                                                 application name for
                                                 target kvrocks cluster,
                                                 eg. myCluster1 or
                                                 testCluster
 -N,--down_shard_node                            set target shard node
                                                 down
 -O,--redo_one_job_by_id_force                   redo one job by one job
                                                 log id ignore done and
                                                 result is ok, eg. -R=1 -O
 -p,--password <arg>                             kvrocks server password
 -P,--port <arg>                                 target node port
 -Q,--quit                                       quit console
 -r,--replica_index <arg>                        target replica index, eg.
                                                 0 or 1
 -R,--redo_one_job_by_id <arg>                   redo one job by one job
                                                 log id, eg. -R=1
 -S,--shard_slot_range_re_avg <arg>              migrate other slot to
                                                 target primary node so
                                                 this shard slot range
                                                 will be avg, require
                                                 ip:port, eg.
                                                 -S=192.168.99.100:6379
 -s,--shard_index <arg>                          target shard index, eg. 0
                                                 or 1
 -T,--delete_tmp_saved_migrated_slot_log <arg>   delete by job log id, eg.
                                                 -T=* or -T=1,2,3
 -U,--tmp_saved_migrated_slot_log                display tmp saved
                                                 migrated slot log
 -v,--view_shard_node                            view one shard node
                                                 cluster info/nodes/slots
 -V,--view_shard_detail                          view all shard nodes
 -X,--x_session_current_variables                view args for reuse in
                                                 current session
 -Y,--up_shard_node                              set target shard node up
 -z,--lazy_migrate                               do not migrate slots
                                                 after adding one shard
                                                 node to cluster 
```

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

Add some primary shard nodes from beginning:

```shell script 
-b=192.168.99.100:6379,192.168.99.100:6380
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
