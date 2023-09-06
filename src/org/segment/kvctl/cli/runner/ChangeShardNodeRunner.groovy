package org.segment.kvctl.cli.runner

import org.segment.d.Record
import org.segment.kvctl.App
import org.segment.kvctl.cli.CommandTaskRunnerHolder
import org.segment.kvctl.cli.TablePrinter
import org.segment.kvctl.db.JobLogDTO
import org.segment.kvctl.shard.ShardNode
import org.slf4j.LoggerFactory

def h = CommandTaskRunnerHolder.instance
def log = LoggerFactory.getLogger(this.getClass())

h.add('change shard node runner') { cmd ->
    '''
forget_node
meet_node
'''.readLines().collect { it.trim() }.findAll { it }.any {
        cmd.hasOption(it)
    }
} { cmd ->
    def app = App.instance
    def numberPat = ~/^\d+$/

    if (cmd.hasOption('forget_node')) {
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

        def shardDetail = app.shardDetail
        if (!shardDetail) {
            log.warn 'no shard exists'
            return
        }

        def shardNode = shardDetail.findShardNodeByIpPort(ip, port as int, false)
        if (!shardNode) {
            log.warn 'no shard node found'
            return
        }

        if (shardDetail.shards.size() == 1 && shardNode.isPrimary) {
            log.warn 'can not delete primary as only one shard left'
            return
        }

        // check undone job log
        def undoJobLog = new JobLogDTO(appId: app.id, isOk: false).one()
        if (undoJobLog) {
            log.warn 'there is undone job'
            TablePrinter.printRecord((Record) undoJobLog)
            return
        }

        app.forgetNode(shardNode)
        return
    }

    if (cmd.hasOption('meet_node')) {
        def ip = cmd.getOptionValue('ip')
        def port = cmd.getOptionValue('port')
        def shardIndex = cmd.getOptionValue('shard_index')
        def isPrimary = cmd.hasOption('is_primary')
        def replicaIndex = cmd.getOptionValue('replica_index')

        if (!ip) {
            log.warn 'ip required'
            return
        }

        if (!port || !port.matches(numberPat)) {
            log.warn 'port must be a number'
            return
        }

        if (!shardIndex || !shardIndex.matches(numberPat)) {
            log.warn 'shard index must be a number'
            return
        }

        if (!isPrimary && !replicaIndex) {
            log.warn 'it is not a primary node need replica index'
            return
        }

        if (replicaIndex && !replicaIndex.matches(numberPat)) {
            log.warn 'replica index must be a number'
            return
        }

        if (!App.isPortListening(port as int, ip)) {
            log.error 'target ip port is not listening'
            return
        }

        def shardNode = new ShardNode()
        shardNode.ip = ip
        shardNode.port = port as int
        shardNode.shardIndex = shardIndex as int
        shardNode.replicaIndex = isPrimary ? 0 : replicaIndex as int
        shardNode.isPrimary = isPrimary

        // check undone job log
        def undoJobLog = new JobLogDTO(appId: app.id, isOk: false).one()
        if (undoJobLog) {
            log.warn 'there is undone job'
            TablePrinter.printRecord((Record) undoJobLog)
            return
        }

        app.meetNode(shardNode, cmd.hasOption('lazy_migrate'))
        return
    }
}
