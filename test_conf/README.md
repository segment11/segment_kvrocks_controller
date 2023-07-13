# Test using redis or kvrocks

## redis

### start redis-server

```bash
cd r1 && redis-server 1.conf
cd r2 && redis-server 2.conf
```

### change src/conf.properties

```
app.engine.isRedis=1
```

### build jar and run

```bash
gradle jar
cp src/conf.properties build/libs
cd build/libs
java -jar segment_kvrocks_controller-1.0.jar
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