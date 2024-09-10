package org.segment.kvctl.operator

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.segment.kvctl.App
import org.segment.kvctl.Conf
import org.segment.kvctl.ex.JobHandleException
import org.segment.kvctl.jedis.JedisPoolHolder
import org.segment.kvctl.model.MultiSlotRange
import org.segment.kvctl.shard.Shard
import org.segment.kvctl.shard.ShardNode
import redis.clients.jedis.Jedis
import redis.clients.jedis.params.MigrateParams

@CompileStatic
@Slf4j
class RedisDBOperator {
    private static String[] toStringArray(Collection<String> list) {
        def arr2 = list.toArray()
        String[] arr = new String[arr2.length]
        for (int i = 0; i < arr2.length; i++) {
            arr[i] = arr2[i] as String
        }
        arr
    }

    static String getNodeId(String ip, int port) {
        def jedisPool = JedisPoolHolder.instance.create(ip, port)
        JedisPoolHolder.useRedisPool(jedisPool) { jedis ->
            return jedis.clusterMyId()
        }
    }

    // need wait a while after meet, or node id not known
    static void meetNode(ShardNode one, ShardNode newOne) {
        def clusterNode = one.clusterNode()
        def existOne = clusterNode.allClusterNodeList.find { it.ip == newOne.ip && it.port == newOne.port }
        if (existOne) {
            log.warn 'already has new shard node: {}', newOne.uuid()
            return
        }

        one.connectAndGet { jedis ->
            def result = jedis.clusterMeet(newOne.ip, newOne.port)
            log.info 'cluster meet node: {}, result: {}, this node: {}, need wait a while 2s', newOne.uuid(), result, one.uuid()
        }
    }

    static void forgetNode(ShardNode removeShardNode, String removeNodeId) {
        def shardDetail = App.instance.shardDetail
        shardDetail.iterateEach { shardNode ->
            if (shardNode.isDown) {
                log.debug 'this shard node is down: {}', shardNode
                return
            }

            shardNode.connectAndGet { jedis ->
                def result = jedis.clusterForget(removeNodeId)
                log.info 'cluster forget node: {}, result: {}, this node: {}', removeShardNode.uuid(), result, shardNode.uuid()
            }
        }
    }

    // redis save nodes/slot range in local file, just warning if not match
    static void setNodes(Shard shard, ShardNode shardNode) {
        def uuid = shardNode.uuid()
        def clusterNode = shardNode.clusterNode()

        if (!shardNode.isPrimary) {
            def primary = shard.primary()
            def primaryNodeId = shard.nodeId(primary)
            def followNodeId = clusterNode.followNodeId
            if (followNodeId && followNodeId == primaryNodeId) {
                log.info 'skip replica, this node: {}, to primary: {}', uuid, primary.uuid()
            } else {
                // never happen, App meet node already replicate and check offset ok

                if (followNodeId) {
                    throw new IllegalStateException('cluster node follow node id not match, expect: ' +
                            primaryNodeId + ', but: ' + followNodeId)
                } else {
                    shardNode.connectAndGet { jedis ->
                        def result = jedis.clusterReplicate(primaryNodeId)
                        log.info 'set replica, this node: {}, to primary: {}, result: {}', uuid, primary.uuid(), result
                        if ('OK' != result) {
                            throw new JobHandleException('set replica fail, result: ' + result +
                                    ', this node: ' + uuid + ', to primary: ' + primary.uuid())
                        }
                    }
                }
            }
            return
        }

        // if primary

        def multiSlotRange = shard.multiSlotRange
        def multiSlotRangeExist = clusterNode.multiSlotRange
        if (multiSlotRange.toString() == multiSlotRangeExist.toString()) {
            log.info 'slot range already match, this node: {}', uuid
            return
        }

        def setInTree = multiSlotRange.toTreeSet()
        def setInHash = multiSlotRange.toHashSet()

        def setExistInTree = multiSlotRangeExist.toTreeSet()
        def setExistInHash = multiSlotRangeExist.toHashSet()

        List<Integer> needAddSlotList = []
        List<Integer> needDeleteSlotList = []

        for (slot in setInTree) {
            if (!(slot in setExistInHash)) {
                needAddSlotList << slot
            }
        }

        for (slot in setExistInTree) {
            if (!(slot in setInHash)) {
                needDeleteSlotList << slot
            }
        }

        if (needDeleteSlotList) {
            log.warn 'why need delete slot, migrate fail? need delete slots: {}, this node: {}', needDeleteSlotList, uuid
        }

        if (needAddSlotList) {
            def arr = MultiSlotRange.toIntArray(needAddSlotList)
            if (setExistInTree) {
                log.warn 'why need add slot, migrate fail? need add slots: {}, this node: {}', arr, uuid
            } else {
                shardNode.connectAndGet { jedis ->
                    def result = jedis.clusterAddSlots(arr)
                    log.info 'cluster add slots size: {}, result: {}, this node: {}', arr.length, result, uuid
                    if ('OK' != result) {
                        throw new JobHandleException('add slots fail, result: ' + result + ', this node: ' + uuid)
                    }
                }
            }
        }
    }

    private static void waitUntilMigrateSuccess(Jedis jedis, Integer slot, String toIp, Integer toPort, String fromUuid) {
        int onceKeyNumber = Conf.instance.getInt('job.migrate.keys.once.number', 100)

        List<String> keyList
        keyList = jedis.clusterGetKeysInSlot(slot, onceKeyNumber)

        def password = App.instance.password

        int count = 0
        while (keyList) {
            count += keyList.size()
            def arr = toStringArray(keyList)
            def result = jedis.migrate(toIp, toPort, 0, new MigrateParams().auth(password), arr)
            if ('OK' != result) {
                throw new JobHandleException('migrate slot fail, result: ' + result +
                        ', this node: ' + fromUuid + ', slot: ' + slot)
            }
        }
        log.debug 'done migrate slot: {}, key number: {}, from node: {}, to node: {}',
                slot, count, fromUuid, toIp + ':' + toPort
        log.info 'done slot: {}', slot
    }

    static void migrateSlot(Jedis jedis, Jedis jedisTo, Integer slot,
                            String fromNodeId, String toNodeId,
                            String toIp, Integer toPort) {
        def resultImport = jedisTo.clusterSetSlotImporting(slot, fromNodeId)
        log.debug 'prepare import slot: {}, result: {}', slot, resultImport

        def result = jedis.clusterSetSlotMigrating(slot, toNodeId)
        log.debug 'prepare migrate slot: {}, result: {}', slot, result

        waitUntilMigrateSuccess(jedis, slot, toIp, toPort, fromNodeId)
        def resultSetSlotFrom = jedis.clusterSetSlotNode(slot, toNodeId)
        log.debug 'migrate slot: {}, from node result: {}', slot, resultSetSlotFrom
        def resultSetSlotTo = jedisTo.clusterSetSlotNode(slot, toNodeId)
        log.debug 'migrate slot: {}, to node result: {}', slot, resultSetSlotTo
    }

    static void setSlotAll(Integer slot, String toNodeId) {
        def shardDetail = App.instance.shardDetail
        shardDetail.iterateEach { shardNode ->
            if (shardNode.isDown) {
                log.debug 'this shard node is down: {}', shardNode
                return
            }

            shardNode.connectAndGet { jedis ->
                jedis.clusterSetSlotNode(slot, toNodeId)
            }
        }
    }

    static void refreshAllShardNode() {
        def shardDetail = App.instance.shardDetail
        for (shard in shardDetail.shards) {
            for (shardNode in shard.nodeList) {
                if (shardNode.isDown) {
                    log.debug 'this shard node is down: {}', shardNode
                    continue
                }

                setNodes(shard, shardNode)
            }
        }
    }

    static void refreshOneShardNodeWhenRestart(ShardNode shardNode) {
        def shardDetail = App.instance.shardDetail
        setNodes(shardDetail.oneShard(shardNode.shardIndex), shardNode)
    }
}
