package org.segment.kvctl.operator

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.segment.kvctl.App
import org.segment.kvctl.ClusterVersionHelper
import org.segment.kvctl.Conf
import org.segment.kvctl.check.StatusCheckResult
import org.segment.kvctl.db.MigrateTmpSaveDTO
import org.segment.kvctl.ex.JobHandleException
import org.segment.kvctl.jedis.ClusterSetCommand
import org.segment.kvctl.jedis.JedisPoolHolder
import org.segment.kvctl.jedis.MessageReader
import org.segment.kvctl.job.task.MigrateSlotsJobTask
import org.segment.kvctl.model.MultiSlotRange
import org.segment.kvctl.shard.ShardNode
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisClusterException

@CompileStatic
@Slf4j
class KvrocksDBOperator {
    static void setNodes(String ip, Integer port, String nodesArgs, Long clusterVersion) {
        def jedisPool = JedisPoolHolder.instance.create(ip, port)
        JedisPoolHolder.useRedisPool(jedisPool) { jedis ->
            def command = new ClusterSetCommand("CLUSTERX")
            byte[] r = jedis.sendCommand(
                    command,
                    "SETNODES".bytes,
                    nodesArgs.bytes,
                    clusterVersion.toString().bytes) as byte[]
            def result = new String(r)
            log.info 'set cluster set nodes {} result: {}', ip + ':' + port, result
            if ('OK' != result) {
                throw new JobHandleException('set cluster set nodes fail, result: ' + result +
                        ', ip/port: ' + ip + ':' + port)
            }
        }
    }

    static void refreshAllShardNode() {
        def appId = App.instance.id
        def shardDetail = App.instance.shardDetail

        // topology
        def allCommandArgs = shardDetail.clusterNodesArgs().join(" \n ")
        log.info allCommandArgs

        // get max cluster version all over every shard node
        List<Long> clusterVersionNow = []
        shardDetail.iterateEach { shardNode ->
            if (!shardNode.isDown) {
                clusterVersionNow << shardNode.currentClusterVersion(true)
            }
        }
        ClusterVersionHelper.instance.getUntil(appId, clusterVersionNow.max())

        def clusterVersion = ClusterVersionHelper.instance.get(appId)

        Set<String> clearedIpPortSet = []
        shardDetail.iterateEach { shardNode ->
            if (shardNode.isDown) {
                log.warn 'this shard node is down: {}', shardNode
                return
            }

            StatusCheckResult checkResult
            try {
                checkResult = shardNode.statusCheck(shardDetail)
            } catch (JedisClusterException e) {
                log.warn e.message
                checkResult = StatusCheckResult.fail(e.message)
            }

            if (checkResult.isOk) {
                log.info 'shard node status check ok, skip refresh: {}', shardNode.uuid()
            } else {
                log.warn 'shard node status check fail, message: {}, refresh: {}', checkResult.message, shardNode.uuid()
                KvrocksDBOperator.setNodes(shardNode.ip, shardNode.port, allCommandArgs, clusterVersion)
            }

            shardNode.clearTmpSaveMigratingSlotValue(appId)
            clearedIpPortSet << shardNode.uuid()
        }

        // if reduce shard, refresh reduced shard nodes cluster nodes for next time rejoin cluster
        for (shard in shardDetail.shardsLastClusterVersion) {
            for (shardNode in shard.nodeList) {
                if (shardNode.isDown) {
                    log.warn 'this shard node is down: {}', shardNode
                    continue
                }
                if (shardNode.uuid() in clearedIpPortSet) {
                    continue
                }

                shardNode.clearTmpSaveMigratingSlotValue(appId)
                // clear all nodes
                // cluster nodes return will have no myself
                KvrocksDBOperator.setNodes(shardNode.ip, shardNode.port, allCommandArgs, clusterVersion)
            }
        }

        // should n = 0
        def n = new MigrateTmpSaveDTO(appId: appId).deleteAll()
        log.info 'delete other tmp save slots range rows: {}', n
        if (n != 0) {
            log.warn 'why n != 0 ?'
        }
    }

    private static void waitUntilMigrateSuccess(Jedis jedis, Integer slot, String fromNodeId) {
        def c = Conf.instance
        // ms
        final int waitMsOnce = c.getInt('job.migrate.wait.once.ms', 10)
        // 2min
        final int waitTimesMax = c.getInt('job.migrate.wait.max', 2 * 60 * 100)

        def info = jedis.clusterInfo()
        def line = info.readLines().find { it.contains('migrating_state:') }

        int cc = 0
        while (!line.contains('success')) {
            cc++
            if (cc >= waitTimesMax) {
                throw new JobHandleException('migrate wait times over max, from node id: ' +
                        fromNodeId + ', slot: ' + slot)
            }

            log.debug 'wait ms: ' + waitMsOnce
            Thread.sleep(waitMsOnce)

            def infoInner = jedis.clusterInfo()
            line = infoInner.readLines().find { it.contains('migrating_state:') }

            // to node may be down
            if (line.contains('fail')) {
                throw new JobHandleException('migrate slot fail, from node id: ' +
                        fromNodeId + ', slot: ' + slot)
            }
        }
        log.info 'done migrate slot: {}', slot
    }

    private static boolean isOffsetOk(String replicationInfo) {
        def kvPairList = MessageReader.fromClusterInfo(replicationInfo)
        if (kvPairList.find { it.key == 'master_link_status' }?.value != 'up') {
            return false
        }

        def v1 = kvPairList.find { it.key == 'master_repl_offset' }?.value
        def v2 = kvPairList.find { it.key == 'slave_repl_offset' }?.value
        if (!v1 || !v2) {
            return false
        }

        v1 == v2
    }

    static void waitUntilOffsetOk(String uuid, Jedis jedis) {
        def c = Conf.instance
        final int waitMsOnce = c.getInt('job.offset.check.wait.once.ms', 100)
        // 5min, may be always not ok because too short
        final int waitTimesMax = c.getInt('job.offset.check.wait.max', 5 * 60 * 10)

        def replicationInfo = jedis.info('replication')
        int cc = 0
        while (!isOffsetOk(replicationInfo)) {
            cc++
            if (cc >= waitTimesMax) {
                throw new JobHandleException('offset check wait times over max, node: ' + uuid)
            }

            if (cc % 100 == 0) {
                log.info 'wait 200ms, loop count: {}', cc
            }
            Thread.sleep(waitMsOnce)

            def infoInner = jedis.info('replication')
            boolean isOffsetOk = isOffsetOk(infoInner)
            if (isOffsetOk) {
                break
            }
        }
        log.info 'done wait offset, node: {}', uuid
    }

    static void migrateSlot(Jedis jedis, Jedis jedisTo, Integer slot, String fromIp, String toIp,
                            String fromNodeId, String toNodeId) {
        byte[] r = jedis.sendCommand(new ClusterSetCommand("CLUSTERX"),
                "MIGRATE".bytes,
                slot.toString().bytes,
                toNodeId.bytes) as byte[]
        def result = new String(r)
        if ('OK' != result) {
            throw new JobHandleException('migrate slot fail, result: ' + result + ', from node id: ' +
                    fromNodeId + ', slot: ' + slot)
        }

        waitUntilMigrateSuccess(jedis, slot, fromNodeId)
    }

    static void setSlot(Jedis jedis, String ip, int port, Integer slot, String nodeId, long clusterVersion) {
        log.debug 'cluster version: {}', clusterVersion
        byte[] r = jedis.sendCommand(new ClusterSetCommand("CLUSTERX"),
                "SETSLOT".bytes,
                slot.toString().bytes,
                "NODE".bytes,
                nodeId.bytes,
                clusterVersion.toString().bytes) as byte[]
        def result = new String(r)
        log.debug 'set slot: {}, ip: {}, port: {}, result: {}', slot, ip, port, result
        if ('OK' != result) {
            throw new JobHandleException('set slot fail, result: ' + result + ', node id: ' + nodeId + ', slot: ' + slot)
        }
    }

    static void setSlotAll(Integer slot, String nodeId,
                           Integer migratedCount, Map<String, Long> currentClusterVersionByIpPort) {
        def shardDetail = App.instance.shardDetail
        for (shard in shardDetail.shards) {
            for (shardNode in shard.nodeList) {
                if (shardNode.isDown) {
                    log.debug 'this shard node is down: {}', shardNode
                    continue
                }

                shardNode.connectAndGet { jedis ->
                    def v = currentClusterVersionByIpPort[shardNode.uuid()]
                    if (v != null) {
                        long newClusterVersion = v + migratedCount
                        KvrocksDBOperator.setSlot(jedis, shardNode.ip, shardNode.port, slot, nodeId, newClusterVersion)
                    }
                }
            }
        }
    }

    // after 2.3.0 version, restart use exists nodes.conf
    @Deprecated
    static void refreshOneShardNodeWhenRestart(ShardNode shardNode) {
        def appId = App.instance.id
        def shardDetail = App.instance.shardDetail

        log.info 'begin refresh cluster nodes for target shard node: {}', shardNode.uuid()
        def migratedSlotSet = shardNode.loadTmpSaveSlotSet(appId, null, MigrateSlotsJobTask.KEY_SLOT_SET_MIGRATING)
        def importedSlotSet = shardNode.loadTmpSaveSlotSet(appId, null, MigrateSlotsJobTask.KEY_SLOT_SET_IMPORTING)
        if (migratedSlotSet) {
            log.info 'migrated slot set: {}', MultiSlotRange.fromSet(migratedSlotSet)
        }
        if (importedSlotSet) {
            log.info 'imported slot set: {}', MultiSlotRange.fromSet(importedSlotSet)
        }

        // other migrating break shard node cluster nodes not right
        // need wait migrating job done and then refresh all cluster nodes right
        def allCommandArgs = shardDetail.clusterNodesArgs(shardNode, migratedSlotSet, importedSlotSet).join(" \n ")
        log.info allCommandArgs

        def shard = shardDetail.oneShard(shardNode.shardIndex)
        def nodeId = shard.nodeId(shardNode)
        shardNode.initNodeId(nodeId)

        def clusterVersion = ClusterVersionHelper.instance.get(appId)
        log.info 'cluster version: {}, app id: {}', clusterVersion, appId
        setNodes(shardNode.ip, shardNode.port, allCommandArgs, clusterVersion)
    }
}
