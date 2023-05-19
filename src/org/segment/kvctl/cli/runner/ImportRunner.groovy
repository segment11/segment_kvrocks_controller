package org.segment.kvctl.cli.runner

import org.segment.kvctl.App
import org.segment.kvctl.ClusterVersionHelper
import org.segment.kvctl.cli.CommandTaskRunnerHolder
import org.segment.kvctl.cli.TablePrinter
import org.segment.kvctl.shard.Shard
import org.segment.kvctl.shard.ShardDetail
import org.segment.kvctl.shard.ShardNode
import org.slf4j.LoggerFactory

def h = CommandTaskRunnerHolder.instance
def log = LoggerFactory.getLogger(this.getClass())

h.add('import runner') { cmd ->
    '''
import
'''.readLines().collect { it.trim() }.findAll { it }.any {
        cmd.hasOption(it)
    }
} { cmd ->
    def app = App.instance
    if (app.shardDetail && app.shardDetail.shards) {
        log.warn 'shard exists, need clear before import'
        return
    }

    // --import=192.168.99.100:6379
    def val = cmd.getOptionValue('import')
    def arr = val.split(':')
    if (arr.length != 2) {
        log.warn 'need ip:port, eg. -I=192.168.99.100:6379'
        return
    }

    def ip = arr[0]
    def port = arr[1]

    def numberPat = ~/^\d+$/

    if (!port || !port.matches(numberPat)) {
        log.warn 'port must be a number'
        return
    }

    if (!App.isPortListening(port as int, ip)) {
        log.error 'target ip port is not listening'
        return
    }

    def firstShardNode = new ShardNode(ip: ip, port: port as int)
    def firstClusterNode = firstShardNode.clusterNode()

    println firstClusterNode.logPretty()

    def allClusterNodeList = firstClusterNode.allClusterNodeList.collect {
        new ShardNode(ip: it.ip, port: it.port).clusterNode()
    }

    // check
    for (cn in allClusterNodeList) {
        if ('ok' != cn.clusterState()) {
            log.warn 'cluster state not ok for {}', cn.uuid()
            return
        }
    }

    def shardDetail = new ShardDetail()
    Set<Long> clusterVersionSet = []

    def primaryClusterNodeList = allClusterNodeList.findAll { it.isPrimary }.sort { it.multiSlotRange }

    int shardIndex = 0
    for (cn in primaryClusterNodeList) {
        clusterVersionSet << cn.clusterVersion()

        def shard = new Shard()
        shard.multiSlotRange = cn.multiSlotRange
        shard.shardIndex = shardIndex

        def primary = new ShardNode()
        primary.shardIndex = shardIndex
        primary.isPrimary = true
        primary.ip = cn.ip
        primary.port = cn.port
        primary.nodeIdFix = cn.nodeId
        shard.nodeList << primary

        def replicaClusterNodeList = allClusterNodeList.findAll { !it.isPrimary && it.followNodeId == cn.nodeId }
        int replicaIndex = 0
        for (rcn in replicaClusterNodeList) {
            clusterVersionSet << rcn.clusterVersion()

            // check if replica slot range is match
            if (rcn.multiSlotRange != cn.multiSlotRange) {
                log.warn 'replica slot range not match for {}', rcn.uuid()
                return
            }

            def replica = new ShardNode()
            replica.shardIndex = shardIndex
            replica.isPrimary = false
            replica.replicaIndex = replicaIndex
            replica.ip = rcn.ip
            replica.port = rcn.port
            replica.nodeIdFix = rcn.nodeId
            shard.nodeList << replica

            replicaIndex++
        }

        shardDetail.shards << shard
        shardIndex++
    }

    shardDetail.version = clusterVersionSet.max()
    ClusterVersionHelper.instance.getUntil(app.id, shardDetail.version)

    app.shardDetail = shardDetail
    app.saveShardDetail()

    List<List<String>> table = shardDetail.logPretty()
    TablePrinter.print(table)
}
