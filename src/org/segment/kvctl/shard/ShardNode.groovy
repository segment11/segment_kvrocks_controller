package org.segment.kvctl.shard

import com.fasterxml.jackson.annotation.JsonIgnore
import groovy.transform.AutoClone
import groovy.transform.CompileStatic
import groovy.transform.ToString
import groovy.transform.TupleConstructor
import org.segment.kvctl.App
import org.segment.kvctl.check.StatusCheckResult
import org.segment.kvctl.db.MigrateTmpSaveDTO
import org.segment.kvctl.jedis.JedisCallback
import org.segment.kvctl.jedis.JedisPoolHolder
import org.segment.kvctl.jedis.MessageReader
import org.segment.kvctl.job.task.MigrateSlotsJobTask
import org.segment.kvctl.model.ClusterNode
import org.segment.kvctl.model.MultiSlotRange
import org.slf4j.LoggerFactory
import redis.clients.jedis.exceptions.JedisClusterException

import static org.segment.kvctl.check.StatusCheckResult.fail
import static org.segment.kvctl.check.StatusCheckResult.ok

@CompileStatic
@TupleConstructor
@ToString(includeNames = true, includePackage = false)
@AutoClone
class ShardNode {
    Boolean isPrimary
    Integer replicaIndex
    String ip
    Integer port
    // for easy get
    Integer shardIndex
    Boolean isDown

    String nodeIdFix

    // if kvrocks process is down when migrating slots, restart process could not just refresh cluster nodes
    // need load these and compare with shard detail target shard slots range, refer App.refreshOneShardNodeWhenRestart
    @JsonIgnore
    TreeSet<Integer> migratingSlotSet
    @JsonIgnore
    TreeSet<Integer> importingSlotSet

    String uuid() {
        ip + ':' + port
    }

    List<String> logPretty() {
        List<String> r = []
        r << (shardIndex != null ? shardIndex.toString() : '')
        r << (ip ?: '')
        // port is not 0
        r << (port ? port.toString() : '')
        r << (isPrimary ? isPrimary.booleanValue().toString() : 'false')
        r << (replicaIndex != null ? replicaIndex.toString() : '')
        r << (isDown ? isDown.booleanValue().toString() : 'false')
        r
    }

    Object connectAndGet(JedisCallback callback) {
        def jedisPool = JedisPoolHolder.instance.create(ip, port)
        JedisPoolHolder.useRedisPool(jedisPool, callback)
    }

    ClusterNode clusterNode() {
        new ClusterNode(ip, port).read()
    }

    // return true -> cluster nodes already set (jedis.clusterNodes do not throw exception)
    boolean initNodeId(String nodeId) {
        new ClusterNode(ip, port).init(nodeId)
    }

    Long currentClusterVersion(boolean doCatchEx = false) {
        connectAndGet { jedis ->
            try {
                def list = MessageReader.fromClusterInfo(jedis.clusterInfo())
                list.find { it.key == 'cluster_my_epoch' }.value as Long
            } catch (JedisClusterException e) {
                if (!doCatchEx) {
                    throw e
                } else {
                    return 0
                }
            }
        } as Long
    }

    StatusCheckResult statusCheck(ShardDetail shardDetail) {
        if (!App.isPortListening(port, ip)) {
            return fail('target ip port is not listening')
        }

        def cn = clusterNode()

        if ('ok' != cn.clusterState()) {
            return fail('cluster state not ok')
        }

        def ips = shardDetail.ips()
        if (ips.size().toString() != cn.clusterInfoValue('cluster_known_nodes')) {
            return fail('cluster_known_nodes not match')
        }

        for (one in cn.allClusterNodeList) {
            def localOne = shardDetail.findShardNodeByIpPort(one.ip, one.port, false)
            if (!localOne) {
                return fail('cluster node not found in local shard detail: ' + one.uuid())
            }

            // check role
            if (localOne.isPrimary != one.isPrimary) {
                return fail('cluster node role not match, expect: ' +
                        (localOne.isPrimary ? 'master' : 'slave') + ', ip/port: ' + one.uuid())
            }

            def localShard = shardDetail.findShardByIpPort(one.ip, one.port)
            // check slave follow
            if (!localOne.isPrimary) {
                def primary = localShard.primary()
                if (primary.ip != one.followNodeIp || primary.port != one.followNodePort) {
                    return fail('cluster node slave follow ip/port not match, expect: ' + primary.uuid() + ', but: ' +
                            one.followNodeIp + ':' + one.followNodePort)
                }
            }

            // check node id
            def localNodeId = localShard.nodeId(localOne)
            if (localNodeId != one.nodeId) {
                return fail('cluster node id not match, expect: ' + localNodeId + ', but: ' + one.nodeId)
            }
        }

        // check slot range
        if (isPrimary) {
            def shard = shardDetail.findShardByIpPort(ip, port)
            def multiSlotRange = shard.multiSlotRange
            if (!cn.multiSlotRange?.list) {
                // not null
                if (multiSlotRange && multiSlotRange.list) {
                    return fail('cluster node slot range not match, expect: null, but: ' + multiSlotRange.toString())
                }
            } else {
                if (multiSlotRange.toString() != cn.multiSlotRange.toString()) {
                    return fail('cluster node slot range not match, expect: ' + multiSlotRange.toString() + ', but: ' +
                            cn.multiSlotRange.toString())
                }
            }
        }

        ok()
    }

    void clearTmpSaveMigratingSlotValue(Integer appId, Integer jobLogId = null) {
        def log = LoggerFactory.getLogger(ShardNode.class)

        // query and delete only for log
        def list = new MigrateTmpSaveDTO(appId: appId, ip: ip, port: port).list()
        for (one in list) {
            if (!jobLogId || jobLogId == one.jobLogId) {
                new MigrateTmpSaveDTO(id: one.id).delete()
            }
            if (one.type == MigrateSlotsJobTask.KEY_SLOT_SET_MIGRATING) {
                log.info 'shard node delete tmp migrating slot value, job log id: {}, node: {}, slots: {}',
                        one.jobLogId, uuid(), one.slotRangeValue
            } else if (one.type == MigrateSlotsJobTask.KEY_SLOT_SET_IMPORTING) {
                log.info 'shard node delete tmp importing slot value, job log id: {}, node: {}, slots: {}',
                        one.jobLogId, uuid(), one.slotRangeValue
            }
        }
    }

    TreeSet<Integer> loadTmpSaveSlotSet(Integer appId, Integer jobLogId, String type) {
        def log = LoggerFactory.getLogger(ShardNode.class)

        TreeSet<Integer> set = []

        def list = new MigrateTmpSaveDTO(appId: appId, jobLogId: jobLogId, ip: ip, port: port, type: type).list()
        for (one in list) {
            log.info 'load from db, job id: {}, val: {}', jobLogId, one.slotRangeValue
            def savedSet = MultiSlotRange.fromSelfString(one.slotRangeValue).toTreeSet()
            set.addAll savedSet
            log.info 'done load db, job id: {}, size: {}', jobLogId, savedSet.size()
        }

        set
    }
}
