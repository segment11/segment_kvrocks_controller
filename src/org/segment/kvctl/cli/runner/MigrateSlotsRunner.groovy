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

h.add('migrate slots runner') { cmd ->
    '''
migrate_slots
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

    def numberPat = ~/^\d+$/

    def ip = cmd.getOptionValue('ip')
    def port = cmd.getOptionValue('port')

    if (!ip) {
        log.warn 'ip required'
        return
    }

    if (!port || !port.matches(numberPat)) {
        log.warn 'port must be a number'
        return
    }

    def shardNode = shardDetail.findShardNodeByIpPort(ip, port as int, false)
    if (!shardNode) {
        log.warn 'no shard node found'
        return
    }

    if (!shardNode.isPrimary) {
        log.warn 'target shard node is not primary'
        return
    }

    def targetShard = shardDetail.oneShard(shardNode.shardIndex)

    def slots = cmd.getOptionValue('migrate_slots')

    TreeSet<Integer> slotSet = []
    for (slot in slots.split(',')) {
        if (slot.contains('-')) {
            def arr = slot.split('-')
            if (arr.length != 2) {
                log.warn 'slot range must has begin-end, eg. 10-20'
                return
            }

            def begin = arr[0]
            def end = arr[1]

            if (!begin.matches(numberPat) || !end.matches(numberPat)) {
                log.warn 'slot range must be a number: {}', slot
                return
            }

            for (i in (begin as int)..(end as int)) {
                slotSet << i
            }
        } else {
            if (!slot.matches(numberPat)) {
                log.warn 'slot must be a number: {}', slot
                return
            }
            slotSet << (slot as int)
        }
    }

    List<MigrateSlotsJobTask> r = []

    for (shard in app.shardDetail.shards) {
        def primary = shard.primary()
        def isSelfShard = primary.uuid() == shardNode.uuid()

        def thisShardSlotSetInHash = shard.slotHashSet()

        TreeSet<Integer> needMigrateSlotSet = []
        for (slot in slotSet) {
            if (slot in thisShardSlotSetInHash) {
                needMigrateSlotSet << slot
            }
        }

        if (needMigrateSlotSet) {
            if (isSelfShard) {
                log.warn 'these slots: {} are already in target shard node: {}', needMigrateSlotSet, shardNode.uuid()
                continue
            }

            def multi = MultiSlotRange.fromSet(needMigrateSlotSet)

            for (slotRange in multi.list) {
                def task = new MigrateSlotsJobTask('migrate some slots sub task')
                task.fromIp = primary.ip
                task.fromPort = primary.port
                task.fromNodeId = shard.nodeId(primary)
                task.toIp = shardNode.ip
                task.toPort = shardNode.port
                task.toNodeId = targetShard.nodeId(shardNode)
                task.beginSlot = slotRange.begin
                task.endSlot = slotRange.end
                task.isAddShard = false

                r << task
            }
        }
    }

    for (task in r) {
        def result = task.run()
        if (!result.isOk) {
            throw new JobHandleException('migrate slots job task run failed, abort - ' + task.stepAsUuid())
        }

        // update from and to shard slot range
        def toShard = shardDetail.findShardByIpPort(task.toIp, task.toPort)
        toShard.multiSlotRange.addMerge(task.beginSlot, task.endSlot)

        def fromShard = shardDetail.findShardByIpPort(task.fromIp, task.fromPort)
        fromShard.multiSlotRange.removeMerge(task.beginSlot, task.endSlot)

        app.saveShardDetail()

        log.info 'done sub task: {}', task.stepAsUuid()

        List<List<String>> table = shardDetail.logPretty()
        TablePrinter.print(table)
    }
}
