package org.segment.kvctl.job.task

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.ObjectMapper
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.segment.kvctl.App
import org.segment.kvctl.ClusterVersionHelper
import org.segment.kvctl.Conf
import org.segment.kvctl.db.JobLogDTO
import org.segment.kvctl.db.MigrateTmpSaveDTO
import org.segment.kvctl.jedis.JedisPoolHolder
import org.segment.kvctl.job.JobResult
import org.segment.kvctl.model.MultiSlotRange
import org.segment.kvctl.operator.KvrocksDBOperator
import org.segment.kvctl.operator.RedisDBOperator
import org.segment.kvctl.shard.ShardNode
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisDataException

@CompileStatic
@Slf4j
class MigrateSlotsJobTask extends AbstractJobTask {
    // for json read
    MigrateSlotsJobTask() {}

    MigrateSlotsJobTask(String step) {
        this.step = step
    }

    String fromIp
    Integer fromPort
    String fromNodeId
    String toIp
    Integer toPort
    String toNodeId

    Integer beginSlot
    Integer endSlot

    ShardNode fromShardNode
    ShardNode toShardNode

    boolean isAddShard = true

    boolean ignoreLocalTest = false

    static final String PARAM_KEY_DONE_COUNT = 'done_count'
    static final String PARAM_KEY_MYSELF = 'json_as_me_as_task'

    static final String KEY_SLOT_SET_MIGRATING = '_m'
    static final String KEY_SLOT_SET_IMPORTING = '_i'

    @Override
    String stepAsUuid() {
        "${fromIp}:${fromPort}->${toIp}:${toPort} slots: ${beginSlot}-${endSlot}".toString()
    }

    @Override
    String toString() {
        stepAsUuid()
    }

    @JsonIgnore
    String innerJsonString

    String toJsonForReload() {
        if (innerJsonString) {
            return innerJsonString
        }
        def om = new ObjectMapper()
        innerJsonString = om.writeValueAsString(this)
        innerJsonString
    }

    static MigrateSlotsJobTask reloadFromJson(String jsonString) {
        def om = new ObjectMapper()
        om.readValue(jsonString, MigrateSlotsJobTask)
    }

    @Override
    JobResult doTask() {
        log.info stepAsUuid()

        def app = App.instance
        def shardDetail = app.shardDetail

        // if reload from json
        if (!fromShardNode) {
            fromShardNode = shardDetail.findShardNodeByIpPort(fromIp, fromPort)
        }
        if (!toShardNode) {
            toShardNode = shardDetail.findShardNodeByIpPort(toIp, toPort)
        }

        log.info 'is add shard: {}', isAddShard

        // save json first
        new JobLogDTO(id: jobLogId, message: 'persist myself for reload').
                addParam(PARAM_KEY_MYSELF, toJsonForReload()).update()

        def doneCountSaved = jobLog.param(PARAM_KEY_DONE_COUNT)
        int doneCount = doneCountSaved ? doneCountSaved as int : 0
        log.info 'already migrate some when last job task handle, slot migrate number: {}', doneCount

        def jedisPool = JedisPoolHolder.instance.create(fromIp, fromPort)
        def jedisPoolTo = JedisPoolHolder.instance.create(toIp, toPort)
        Jedis jedis = jedisPool.resource
        Jedis jedisTo = jedisPoolTo.resource

        def fromMultiSlotRange = MultiSlotRange.fromClusterSlots(jedis.clusterSlots(), fromNodeId)
        def toMultiSlotRange = MultiSlotRange.fromClusterSlots(jedisTo.clusterSlots(), toNodeId)
        log.info 'from slot range: {}, node: {}', fromMultiSlotRange, fromShardNode.uuid()
        log.info 'to slot range: {}, node: {}', toMultiSlotRange, toShardNode.uuid()

        Map<String, Long> currentClusterVersionByIpPort = [:]
        shardDetail.iterateEach { shardNode ->
            currentClusterVersionByIpPort[shardNode.uuid()] = shardNode.currentClusterVersion()
        }
        long maxClusterVersion = 0
        for (entry in currentClusterVersionByIpPort) {
            if (maxClusterVersion < entry.value) {
                maxClusterVersion = entry.value
            }
        }
        log.info 'current cluster versions:'
        currentClusterVersionByIpPort.each { k, v ->
            log.info k + ' - ' + v
        }

        loadMigratingSlotSet(fromShardNode)
        loadMigratingSlotSet(toShardNode)

        def isEngineRedis = Conf.instance.isOn('app.engine.isRedis')
        def isEngineVelo = Conf.instance.isOn('app.engine.isVelo')
        def isLocalTest = !isEngineRedis && Conf.instance.isOn('isLocalTest') && !ignoreLocalTest

        int loopCount = 0
        int migratedCount = 0
        try {
            for (int slot = beginSlot; slot <= endSlot; slot++) {
                loopCount++

                if (!fromMultiSlotRange.contains(slot)) {
                    if (toMultiSlotRange.contains(slot)) {
                        log.warn 'slot already migrate: {}', slot
                    } else {
                        log.warn 'slot fail migrate: {}, from ip/port: {}:{}, to ip/port: {}:{}, both has not this slot',
                                slot, fromIp, fromPort, toIp, toPort
                    }
                    continue
                }

                if ((slot in toShardNode.importingSlotSet) || (slot in fromShardNode.migratingSlotSet)) {
                    log.warn 'slot already migrate in tmp save: {}', slot
                    continue
                }

                try {
                    if (isEngineRedis) {
                        RedisDBOperator.migrateSlot(jedis, jedisTo, slot, fromNodeId, toNodeId, toIp, toPort)
                    } else {
                        // for velo
                        if (isEngineVelo) {
                            KvrocksDBOperator.migrateFromSlot(jedisTo, slot, fromIp, fromPort)
                        }
                        KvrocksDBOperator.migrateSlot(jedis, jedisTo, slot, fromIp, toIp, fromNodeId, toNodeId)
                    }
                    migratedCount++

                    // cluster version holder need increase, so when next time set cluster version is biggest
                    ClusterVersionHelper.instance.getUntil(app.id, maxClusterVersion + migratedCount)

                    // broadcast
                    if (isEngineRedis) {
                        RedisDBOperator.setSlotAll(slot, toNodeId)
                    } else {
                        // tips: kvrocks cluster version need continue +1
                        KvrocksDBOperator.setSlotAll(slot, toNodeId, migratedCount, currentClusterVersionByIpPort)
                    }

                    tmpSaveMigratedSlot(slot, fromShardNode, toShardNode)

                    if (loopCount % 100 == 0) {
                        log.info 'loop count: {}, migrated count: {}', loopCount, migratedCount
                        // for job log show, not just Running
                        new JobLogDTO(id: jobLogId, message: 'slots migrate number: ' + loopCount).
                                addParam(PARAM_KEY_MYSELF, innerJsonString).
                                addParam(PARAM_KEY_DONE_COUNT, loopCount.toString()).update()

                        if (isLocalTest) {
                            log.warn 'local test, skip migrate other slots'
                            break
                        }
                    }
                } catch (JedisDataException e) {
                    if (e.message.contains('migrate slot which has been migrated')) {
                        // ignore
                        log.info 'slot is migrated already, slot: {}', slot
                    } else {
                        throw e
                    }
                }
            }
        } finally {
            jedis.close()
            jedisTo.close()
        }

        JobResult.ok('slots migrate number: ' + loopCount)
    }

    private void loadMigratingSlotSet(ShardNode shardNode) {
        def app = App.instance
        def migratedSlotSet = shardNode.loadTmpSaveSlotSet(app.id, jobLogId, KEY_SLOT_SET_MIGRATING)
        def importedSlotSet = shardNode.loadTmpSaveSlotSet(app.id, jobLogId, KEY_SLOT_SET_IMPORTING)

        if (shardNode.migratingSlotSet == null) {
            shardNode.migratingSlotSet = new TreeSet<Integer>()
        }
        if (shardNode.importingSlotSet == null) {
            shardNode.importingSlotSet = new TreeSet<Integer>()
        }

        shardNode.migratingSlotSet.addAll(migratedSlotSet)
        shardNode.importingSlotSet.addAll(importedSlotSet)
    }

    private MigrateTmpSaveDTO savedMigrate
    private MigrateTmpSaveDTO savedImport

    private void tmpSaveMigratedSlot(Integer slot, ShardNode fromNode, ShardNode toNode) {
        def app = App.instance

        fromNode.migratingSlotSet << slot
        toNode.importingSlotSet << slot

        def rowMigrate = new MigrateTmpSaveDTO(appId: app.id, jobLogId: jobLogId)
        rowMigrate.ip = fromIp
        rowMigrate.port = fromPort
        rowMigrate.type = KEY_SLOT_SET_MIGRATING

        if (!savedMigrate) {
            savedMigrate = rowMigrate.one()
        }
        if (savedMigrate) {
            // in loop worst O(n**2), if continue 0(n)
            savedMigrate.slotRangeValue = MultiSlotRange.fromSet(fromNode.migratingSlotSet).toString()
            savedMigrate.updatedDate = new Date()
            savedMigrate.update()
        } else {
            rowMigrate.slotRangeValue = MultiSlotRange.fromSet(fromNode.migratingSlotSet).toString()
            rowMigrate.updatedDate = new Date()
            def savedId = rowMigrate.add()
            savedMigrate = rowMigrate
            savedMigrate.id = savedId
        }

        def rowImport = new MigrateTmpSaveDTO(appId: app.id, jobLogId: jobLogId)
        rowImport.ip = toIp
        rowImport.port = toPort
        rowImport.type = KEY_SLOT_SET_IMPORTING

        if (!savedImport) {
            savedImport = rowImport.one()
        }
        if (savedImport) {
            // in loop worst O(n**2), if continue 0(n)
            savedImport.slotRangeValue = MultiSlotRange.fromSet(toNode.importingSlotSet).toString()
            savedImport.updatedDate = new Date()
            savedImport.update()
        } else {
            rowImport.slotRangeValue = MultiSlotRange.fromSet(toNode.importingSlotSet).toString()
            rowImport.updatedDate = new Date()
            def savedId = rowImport.add()
            savedImport = rowImport
            savedImport.id = savedId
        }
    }
}
