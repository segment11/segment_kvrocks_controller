# Test using redis or kvrocks

## redis

### start redis-server

```bash
cd r1 && redis-server 1.conf
cd r2 && redis-server 2.conf
```

### build jar and run

```bash
gradle jar
cd build/libs
java -jar segment_kvrocks_controller-1.0.jar app.engine.isRedis=1
```

### test

```in java shell
-a -m -s=0 --ip=127.0.0.1 --port=6379 --name=test
-V
-A
-c
-a -m -s=1 --ip=127.0.0.1 --port=6380
-V
-A
-c
-d
-V
-a -s=0 -r=0 --port=6380
-V
-A
-c
-Q
```

## kvrocks

### start kvrocks

```bash
cd r1 && docker run -d --name=kvrocks1 --net=host -v `pwd`/1.conf:/var/lib/kvrocks/kvrocks.conf:ro apache/kvrocks
cd r2 && docker run -d --name=kvrocks2 --net=host -v `pwd`/2.conf:/var/lib/kvrocks/kvrocks.conf:ro apache/kvrocks
cd r3 && docker run -d --name=kvrocks3 --net=host -v `pwd`/3.conf:/var/lib/kvrocks/kvrocks.conf:ro apache/kvrocks
```

### build jar and run

```bash
gradle jar
cd build/libs
java -jar segment_kvrocks_controller-1.0.jar app.engine.isRedis=0
```

### test

```in java shell
-a -m -s=0 --ip=127.0.0.1 --port=6379 --name=test
-V
-A
-a -m -s=1 -z --ip=127.0.0.1 --port=6380
-c
-V
-A
-a -m -s=2 -z --ip=127.0.0.1 --port=6381
-c
-V
-A
-S=127.0.0.1:6380
-V
-A
-S=127.0.0.1:6381
-V
-A
-Q
```