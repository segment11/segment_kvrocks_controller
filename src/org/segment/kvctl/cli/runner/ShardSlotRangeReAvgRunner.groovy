package org.segment.kvctl.cli.runner

import org.segment.kvctl.App
import org.segment.kvctl.cli.CommandTaskRunnerHolder
import org.segment.kvctl.cli.TablePrinter
import org.segment.kvctl.ex.JobHandleException
import org.segment.kvctl.job.task.MigrateSlotsJobTask
import org.segment.kvctl.model.MultiSlotRange
import org.slf4j.LoggerFactory

def h = CommandTaskRunnerHolder.instance
def log = LoggerFactory.getLogger(this.getClass())

h.add('shard slot range re avg runner') { cmd ->
    '''
shard_slot_range_re_avg
'''.readLines().collect { it.trim() }.findAll { it }.any {
        cmd.hasOption(it)
    }
} { cmd ->
    def app = App.instance
    def shardDetail = app.shardDetail
    if (!shardDetail) {
        log.warn 'no shard exists'
        return
    }

    // --shard_slot_range_re_avg=192.168.99.100:6379
    def val = cmd.getOptionValue('shard_slot_range_re_avg')
    def arr = val.split(':')
    if (arr.length != 2) {
        log.warn 'need ip:port, eg. -S=192.168.99.100:6379'
        return
    }

    def ip = arr[0]
    def port = arr[1]

    def numberPat = ~/^\d+$/

    if (!port || !port.matches(numberPat)) {
        log.warn 'port must be a number'
        return
    }

    def shardNode = shardDetail.findShardNodeByIpPort(ip, port as int, false)
    if (!shardNode) {
        log.warn 'no shard node found'
        return
    }

    if (!App.isPortListening(port as int, ip)) {
        log.error 'target ip port is not listening'
        return
    }

    if (!shardNode.isPrimary) {
        log.warn 'target shard node is not primary'
        return
    }

    def shardIndex = shardNode.shardIndex

    List<MigrateSlotsJobTask> r = []

    def localShard = shardDetail.oneShard(shardIndex)
    def localShardSlotsInTree = localShard.slotTreeSet()

    def clone = shardDetail.deepClone()
    clone.splitSlots()
    def cloneShard = clone.oneShard(shardIndex)
    log.info 'slot range should change to: {}', cloneShard.multiSlotRange.toString()
    // local shard should change be this
    def cloneShardSlotsInHash = cloneShard.slotHashSet()

    for (otherShard in shardDetail.shards) {
        // only consider other shards
        if (otherShard.shardIndex == shardIndex) {
            continue
        }

        TreeSet<Integer> needMigrateSlotSet = []
        TreeSet<Integer> needImportSlotSet = []

        def otherShardSlotSetInTree = otherShard.slotTreeSet()
        for (slot in otherShardSlotSetInTree) {
            if (slot in cloneShardSlotsInHash) {
                needImportSlotSet << slot
            }
        }

        def otherCloneShard = clone.oneShard(otherShard.shardIndex)
        def otherCloneShardSlotsInHash = otherCloneShard.slotHashSet()
        for (slot in localShardSlotsInTree) {
            if (slot in otherCloneShardSlotsInHash) {
                needMigrateSlotSet << slot
            }
        }

        def otherShardPrimary = otherShard.primary()
        if (needImportSlotSet) {
            def multi = MultiSlotRange.fromSet(needImportSlotSet)

            for (slotRange in multi.list) {
                def task = new MigrateSlotsJobTask('one shard re avg')
                task.fromIp = otherShardPrimary.ip
                task.fromPort = otherShardPrimary.port
                task.fromNodeId = otherShard.nodeId(otherShardPrimary)

                task.toIp = shardNode.ip
                task.toPort = shardNode.port
                task.toNodeId = localShard.nodeId(shardNode)
                task.beginSlot = slotRange.begin
                task.endSlot = slotRange.end
                task.isAddShard = false
//                task.ignoreLocalTest = true

                r << task
            }
        }

        if (needMigrateSlotSet) {
            def multi = MultiSlotRange.fromSet(needMigrateSlotSet)

            for (slotRange in multi.list) {
                def task = new MigrateSlotsJobTask('one shard re avg')
                task.fromIp = shardNode.ip
                task.fromPort = shardNode.port
                task.fromNodeId = localShard.nodeId(shardNode)
                task.toIp = otherShardPrimary.ip
                task.toPort = otherShardPrimary.port
                task.toNodeId = otherShard.nodeId(otherShardPrimary)
                task.beginSlot = slotRange.begin
                task.endSlot = slotRange.end
                task.isAddShard = false
//                task.ignoreLocalTest = true

                r << task
            }
        }
    }

    if (!r) {
        log.warn 'slot range already match avg: {}', cloneShard.multiSlotRange
        return
    }

    for (task in r) {
        def result = task.run()
        if (!result.isOk) {
            throw new JobHandleException('shard slot range re avg job task run failed, abort - ' + task.stepAsUuid())
        }

        // update from and to shard slot range
        def toShard = shardDetail.findShardByIpPort(task.toIp, task.toPort)
        toShard.multiSlotRange.addMerge(task.beginSlot, task.endSlot)

        def fromShard = shardDetail.findShardByIpPort(task.fromIp, task.fromPort)
        fromShard.multiSlotRange.removeMerge(task.beginSlot, task.endSlot)

        app.saveShardDetail()

        // clear tmp saved, because not refresh all nodes after task run
        task.fromShardNode.clearTmpSaveMigratingSlotValue(app.id, task.jobLogId)
        task.toShardNode.clearTmpSaveMigratingSlotValue(app.id, task.jobLogId)

        log.info 'done sub task: {}', task.stepAsUuid()

        List<List<String>> table = shardDetail.logPretty()
        TablePrinter.print(table)
    }
}
