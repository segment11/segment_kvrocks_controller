package org.segment.kvctl

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.commons.net.telnet.TelnetClient
import org.segment.kvctl.db.AppDTO
import org.segment.kvctl.db.JobLogDTO
import org.segment.kvctl.db.MigrateTmpSaveDTO
import org.segment.kvctl.ex.JobHandleException
import org.segment.kvctl.jedis.MessageReader
import org.segment.kvctl.job.task.MigrateSlotsJobTask
import org.segment.kvctl.model.MultiSlotRange
import org.segment.kvctl.operator.KvrocksDBOperator
import org.segment.kvctl.operator.RedisDBOperator
import org.segment.kvctl.shard.Shard
import org.segment.kvctl.shard.ShardDetail
import org.segment.kvctl.shard.ShardNode
import org.segment.kvctl.shard.SlotBalancer
import redis.clients.jedis.args.ClusterFailoverOption

@CompileStatic
@Slf4j
@Singleton
class App {
    Integer id

    String name

    String password

    ShardDetail shardDetail

    void saveShardDetail() {
        assert id

        def one = new AppDTO()
        one.id = id
        one.shardDetail = shardDetail
        one.updatedDate = new Date()

        one.update()
    }

    void save() {
        assert id

        // clear when migrate slot job failed but already tmp save migrated slots
        shardDetail.iterateEach { shardNode ->
            if (shardNode.migratingSlotSet) {
                shardNode.migratingSlotSet.clear()
            }
            if (shardNode.importingSlotSet) {
                shardNode.importingSlotSet.clear()
            }
        }

        def one = new AppDTO()
        one.id = id
        one.name = name
        one.password = password
        one.shardDetail = shardDetail
        one.updatedDate = new Date()

        one.update()

        // set old job log expired for better view
        def jobLogList = new JobLogDTO(appId: id).list()
        for (jobLog in jobLogList) {
            if (!jobLog.step.contains(' expired')) {
                new JobLogDTO(id: jobLog.id, step: jobLog.step + ' expired').update()
                log.info 'set job log expired, job log id: {}, step: {}', jobLog.id, jobLog.step
            }
        }
    }

    void addNewNodeId(ShardNode oneShardNode, ShardNode newShardNode, String newNodeId) {
        log.info 'ready to refresh cluster nodes for shard node: {}, as new primary shard node come in: {}',
                oneShardNode.uuid(), newShardNode.uuid()

        def isEngineRedis = Conf.instance.isOn('app.engine.isRedis')
        if (isEngineRedis) {
            if (oneShardNode.uuid() == newShardNode.uuid()) {
                return
            }
            RedisDBOperator.meetNode(oneShardNode, newShardNode)
        } else {
            // topology
            // in saveLastVersion closure, so use last version
            List<String> argsList = shardDetail.clusterNodesArgsLastVersion()
            // new add shard set not slot first
            argsList << "${newNodeId} ${newShardNode.ip} ${newShardNode.port} master -".toString()
            String allCommandArgs = argsList.join(" \n ")
            log.info allCommandArgs

            def clusterVersion = ClusterVersionHelper.instance.get(id)
            log.info 'cluster version: {}, app id: {}', clusterVersion, id

            KvrocksDBOperator.setNodes(oneShardNode.ip, oneShardNode.port, allCommandArgs, clusterVersion)
        }

        log.info 'wait a while 2s'
        Thread.sleep(2000)
    }

    // set one shard node new node id so it can do migrate slot if need
    // if isLazyMigrate = true, then not do migrate slots, you can migrate one shard using shard_slot_range_re_avg
    App meetNode(ShardNode addShardNode, boolean isLazyMigrate) {
        assert id

        if (!shardDetail) {
            shardDetail = new ShardDetail()
        }

        def shardIndex = addShardNode.shardIndex

        def beginShardIndex = shardDetail.maxShardIndex() + 1
        if (addShardNode.isPrimary && shardIndex != beginShardIndex) {
            throw new IllegalStateException('new shard index need begin with: ' + beginShardIndex)
        }

        def targetShard = shardDetail.oneShard(shardIndex)

        if (addShardNode.isPrimary) {
            if (targetShard && targetShard.primary()) {
                throw new IllegalStateException('target shard already has primary: ' + targetShard.primary().uuid())
            }
        } else {
            if (!targetShard || !targetShard.primary()) {
                throw new IllegalStateException('target shard not found, need add primary first, shard index: ' + shardIndex)
            }

            def lastReplicaShardNode = targetShard.nodeList.findAll { !it.isPrimary }.max { it.replicaIndex }
            def lastReplica = lastReplicaShardNode ? lastReplicaShardNode.replicaIndex : -1
            if (addShardNode.replicaIndex != lastReplica + 1) {
                throw new IllegalStateException('new replica index need begin with: ' + (lastReplica + 1))
            }
        }

        if (addShardNode.isPrimary) {
            // add primary shard node
            shardDetail.saveLastVersion {
                def newMultiSlotRange = new MultiSlotRange()

                def shardNew = new Shard(shardIndex: shardIndex, multiSlotRange: newMultiSlotRange)
                shardNew.nodeList << addShardNode
                shardDetail.shards << shardNew

                def nodeId = shardNew.nodeId(addShardNode)
                addShardNode.initNodeId(nodeId)

                // first shard, if shard index 0 is removed, new add shard shard index must be > 0
                if (shardIndex == 0) {
                    shardDetail.splitSlots()
                } else {
                    // need migrate
                    // set node id and all other shard node need know this new node id
                    def newNodeId = shardNew.nodeId(addShardNode)
                    // for retry
                    def isClusterNodesSetAlready = addShardNode.initNodeId(newNodeId)
                    if (!isClusterNodesSetAlready) {
                        addNewNodeId(addShardNode, addShardNode, newNodeId)
                    } else {
                        // check if cluster nodes include myself node id
                        addShardNode.connectAndGet { jedis ->
                            def allClusterNodeList = MessageReader.fromClusterNodes(jedis.clusterNodes(), addShardNode.uuid())
                            def myNode = allClusterNodeList.find { it.isMySelf }
                            // why happen ?
                            // delete target node from cluster before, then add it again
                            if (myNode == null) {
                                // set cluster nodes again
                                addNewNodeId(addShardNode, addShardNode, newNodeId)
                            }
                        }
                    }

                    // let other shard nodes know this new shard node (node id)
                    shardDetail.iterateEach { oneShardNode ->
                        if (oneShardNode.uuid() != addShardNode.uuid()) {
                            boolean isKnownThisNewNodeId = oneShardNode.clusterNode().allClusterNodeList.
                                    find { it.nodeId == newNodeId } != null
                            if (!isKnownThisNewNodeId) {
                                addNewNodeId(oneShardNode, addShardNode, newNodeId)
                            }
                        }
                    }

                    if (!isLazyMigrate) {
                        List<MigrateSlotsJobTask> r = []

                        def lastVersionShardSize = shardDetail.shardsLastClusterVersion.size()
                        def needMigrateSlotSize = SlotBalancer.needMigrateSlotSize(lastVersionShardSize)
                        for (shardLastVersion in shardDetail.shardsLastClusterVersion) {
                            def needMigrateSlotSet = shardLastVersion.multiSlotRange.removeSomeFromEnd(needMigrateSlotSize)
                            if (!needMigrateSlotSet) {
                                continue
                            }

                            def primaryLastVersion = shardLastVersion.primary()

                            def tmpMultiSlotRange = MultiSlotRange.fromSet(needMigrateSlotSet)
                            for (slotRange in tmpMultiSlotRange.list) {
                                def task = new MigrateSlotsJobTask('add shard sub task')
                                task.fromIp = primaryLastVersion.ip
                                task.fromPort = primaryLastVersion.port
                                task.fromNodeId = shardLastVersion.nodeId(primaryLastVersion)

                                task.toIp = addShardNode.ip
                                task.toPort = addShardNode.port
                                task.toNodeId = newNodeId
                                task.beginSlot = slotRange.begin
                                task.endSlot = slotRange.end
                                r << task
                            }
                        }

                        for (task in r) {
                            task.isAddShard = true
                            def result = task.run()
                            if (!result.isOk) {
                                throw new JobHandleException('add shard job task run failed, abort - ' + task.stepAsUuid())
                            }

                            // update from and to shard slot range
                            newMultiSlotRange.addMerge(task.beginSlot, task.endSlot)

                            def fromShard = shardDetail.findShardByIpPort(task.fromIp, task.fromPort)
                            fromShard.multiSlotRange.removeMerge(task.beginSlot, task.endSlot)

                            saveShardDetail()
                        }
                    } else {
                        log.warn 'lazy migrate, you can migrate one shard using shard_slot_range_re_avg, eg. -S={}:{}',
                                addShardNode.ip, addShardNode.port
                    }
                }

                refreshAllShardNode()
                save()
            }
        } else {
            // add replica shard node
            shardDetail.saveLastVersion {
                targetShard.nodeList << addShardNode

                def primary = targetShard.primary()
                def primaryNodeId = targetShard.primaryNodeId()

                def isEngineRedis = Conf.instance.isOn('app.engine.isRedis')
                if (isEngineRedis) {
                    shardDetail.iterateEach { shardNode ->
                        RedisDBOperator.meetNode(shardNode, addShardNode)
                    }
                    log.info 'wait a while 2s'
                    Thread.sleep(2000)

                    addShardNode.connectAndGet { jedis ->
                        def uuid = addShardNode.uuid()

                        def result = jedis.clusterReplicate(primaryNodeId)
                        log.info 'set replica, this node: {}, to primary: {}, result: {}', uuid, primary.uuid(), result
                        if ('OK' != result) {
                            throw new JobHandleException('set replica fail, result: ' + result +
                                    ', this node: ' + uuid + ', to primary: ' + primary.uuid())
                        }
                        KvrocksDBOperator.waitUntilOffsetOk(uuid, jedis, primaryNodeId)
                    }
                } else {
                    def nodeId = targetShard.nodeId(addShardNode)
                    def ip = addShardNode.ip
                    def port = addShardNode.port

                    addShardNode.initNodeId(nodeId)

                    // topology
                    List<String> argsList = []
                    def multiSlotRange = targetShard.multiSlotRange
                    // primary
                    argsList.addAll(multiSlotRange.clusterNodesArgs(primaryNodeId, primary.ip, primary.port))
                    // replica
                    argsList << "${nodeId} ${ip} ${port} slave ${primaryNodeId}".toString()

                    String allCommandArgs = argsList.join(" \n ")
                    log.info allCommandArgs

                    def clusterVersion = ClusterVersionHelper.instance.get(id)
                    KvrocksDBOperator.setNodes(ip, port, allCommandArgs, clusterVersion)

                    addShardNode.connectAndGet { jedis ->
                        KvrocksDBOperator.waitUntilOffsetOk(addShardNode.uuid(), jedis, primaryNodeId)
                    }
                }

                refreshAllShardNode()
                save()
            }
        }

        this
    }

    App forgetNode(ShardNode removeShardNode) {
        assert id

        if (!shardDetail) {
            throw new IllegalStateException('no shard exists')
        }

        def targetShard = shardDetail.oneShard(removeShardNode.shardIndex)
        if (!targetShard) {
            throw new IllegalStateException('target shard not found, shard index: ' + removeShardNode.shardIndex)
        }

        def old = targetShard.nodeList.find { it.uuid() == removeShardNode.uuid() }
        if (!old) {
            throw new IllegalStateException('no shard node found')
        }

        if (removeShardNode.isPrimary) {
            def replicaNodeList = targetShard.nodeList.findAll { !it.isPrimary }
            if (replicaNodeList) {
                throw new IllegalStateException('can not delete primary as this shard has replica: ' +
                        replicaNodeList.collect { it.uuid() })
            }

            def removeSlotList = targetShard.multiSlotRange.toList()
            def removeNodeId = targetShard.nodeId(removeShardNode)
            def removeMultiSlotRange = targetShard.multiSlotRange

            // remove primary shard node
            shardDetail.saveLastVersion {
                shardDetail.shards.remove(targetShard)

                List<MigrateSlotsJobTask> r = []

                def thisVersionShardSize = shardDetail.shards.size()
                def slotSetList = SlotBalancer.splitSlotSetForMigrateWhenReduceShard(removeSlotList, thisVersionShardSize)

                int i = 0
                for (shardThisVersion in shardDetail.shards) {
                    def needMigrateSlotSet = slotSetList[i]
                    if (!needMigrateSlotSet) {
                        log.warn 'need not migrate to target shard, as slot range is to small, shard index: {}',
                                shardThisVersion.shardIndex
                    } else {
                        def primaryLastVersion = shardThisVersion.primary()

                        def tmpMultiSlotRange = MultiSlotRange.fromSet(needMigrateSlotSet)
                        for (slotRange in tmpMultiSlotRange.list) {
                            def task = new MigrateSlotsJobTask('reduce shard sub task')
                            task.fromIp = removeShardNode.ip
                            task.fromPort = removeShardNode.port
                            task.fromNodeId = removeNodeId

                            task.toIp = primaryLastVersion.ip
                            task.toPort = primaryLastVersion.port
                            task.toNodeId = shardThisVersion.nodeId(primaryLastVersion)
                            task.beginSlot = slotRange.begin
                            task.endSlot = slotRange.end
                            r << task
                        }
                    }

                    i++
                }

                for (task in r) {
                    task.isAddShard = true
                    def result = task.run()
                    if (!result.isOk) {
                        throw new JobHandleException('reduce shard job task run failed, abort - ' + task.stepAsUuid())
                    }

                    // update from and to shard slot range
                    def toShard = shardDetail.findShardByIpPort(task.toIp, task.toPort)
                    toShard.multiSlotRange.addMerge(task.beginSlot, task.endSlot)

                    removeMultiSlotRange.removeMerge(task.beginSlot, task.endSlot)

                    saveShardDetail()
                }

                def isEngineRedis = Conf.instance.isOn('app.engine.isRedis')
                if (isEngineRedis) {
                    RedisDBOperator.forgetNode(removeShardNode, removeNodeId)
                }

                refreshAllShardNode(removeShardNode)
                save()
            }
        } else {
            // remove replica shard node
            def removeNodeId = targetShard.nodeId(removeShardNode)

            shardDetail.saveLastVersion {
                targetShard.nodeList.remove(old)

                def isEngineRedis = Conf.instance.isOn('app.engine.isRedis')
                if (isEngineRedis) {
                    RedisDBOperator.forgetNode(removeShardNode, removeNodeId)
                }

                refreshAllShardNode(removeShardNode)
                save()
            }
        }

        this
    }

    void failover(Integer shardIndex, Integer replicaIndex) {
        assert id

        if (!shardDetail) {
            throw new IllegalStateException('no shard exists')
        }

        def shard = shardDetail.oneShard(shardIndex)
        if (!shard) {
            throw new IllegalStateException('target shard not found, shard index: ' + shardIndex)
        }

        def shardNode = shard.replica(replicaIndex)
        if (!shardNode) {
            throw new IllegalStateException('target replica node not found, replica index: ' + replicaIndex)
        }

        if (!isPortListening(shardNode.port, shardNode.ip)) {
            log.error 'target replica node ip port is not listening, ip: {}, port: {}', shardNode.ip, shardNode.port
            return
        }

        def primary = shard.primary()
        def isPrimaryDown = !isPortListening(primary.port, primary.ip)

        shardDetail.saveLastVersion {
            primary.isPrimary = false
            primary.replicaIndex = replicaIndex
            primary.isDown = isPrimaryDown

            shardNode.isPrimary = true
            shardNode.replicaIndex = null

            def isEngineRedis = Conf.instance.isOn('app.engine.isRedis')
            if (isEngineRedis) {
                // check if auto failover
                def clusterNode = shardNode.clusterNode()
                if (clusterNode.isPrimary) {
                    log.warn 'already be primary, this node: {}', shardNode.uuid()
                } else {
                    // cluster fail over take over
                    def result = shardNode.connectAndGet { jedis ->
                        jedis.clusterFailover(ClusterFailoverOption.TAKEOVER)
                    } as String
                    log.warn 'cluster failover take over, result: {}, this node: {}', result, shardNode.uuid()
                    log.info 'wait a while 2s'
                    Thread.sleep(2000)
                }
            }

            refreshAllShardNode()
            save()
        }
    }

    static void refreshAllShardNode(ShardNode removeShardNode = null) {
        def isEngineRedis = Conf.instance.isOn('app.engine.isRedis')
        if (isEngineRedis) {
            // redis need not reset slot range
            RedisDBOperator.refreshAllShardNode()
        } else {
            KvrocksDBOperator.refreshAllShardNode(removeShardNode)
        }
    }

    // table app row, this application itself will not be deleted
    App clearAll() {
        deleteLeafAlloc()

        new JobLogDTO(appId: id).deleteAll()
        new MigrateTmpSaveDTO(appId: id).deleteAll()

        id = null
        password = null
        shardDetail = null

        this
    }

    App init(String name, String password, boolean clear) {
        this.name = name
        this.password = password

        // clear = true => table app row, this application itself will be deleted
        if (clear) {
            if (this.id) {
                clearAll()
            }

            new AppDTO(name: name).deleteAll()
            this.shardDetail = null
        }

        def one = new AppDTO(name: name).one()
        if (one) {
            this.id = one.id
            this.shardDetail = one.shardDetail
        } else {
            this.id = new AppDTO(name: name, password: password, updatedDate: new Date()).add()
        }

        createLeafAlloc()
        this
    }

    synchronized void createLeafAlloc() {
        def one = new JobLogDTO()
        def row = one.useD().one('select biz_tag from leaf_alloc where biz_tag = ?', [id])
        if (!row) {
            def insertSql = "insert into leaf_alloc(biz_tag, max_id, step, description) " +
                    "values(?, 1, 100, 'Redis/Kvrocks Application')"
            one.useD().exeUpdate(insertSql, [id])
            log.info 'add leaf alloc for app id: {}', id
        } else {
            log.info 'already exists leaf alloc for app id: {}', id
        }
    }

    synchronized void deleteLeafAlloc() {
        def one = new JobLogDTO()
        def deleteSql = "delete from leaf_alloc where biz_tag = ?"
        one.useD().exeUpdate(deleteSql, [id])
    }

    static boolean isPortListening(Integer port, String host = '127.0.0.1') {
        def tc = new TelnetClient(connectTimeout: 500)
        try {
            tc.connect(host, port)
            return true
        } catch (Exception ignored) {
            return false
        } finally {
            tc.disconnect()
        }
    }
}
