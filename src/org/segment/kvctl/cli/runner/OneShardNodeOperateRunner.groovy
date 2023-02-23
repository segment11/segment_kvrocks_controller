package org.segment.kvctl.cli.runner

import org.segment.kvctl.App
import org.segment.kvctl.Conf
import org.segment.kvctl.cli.CommandTaskRunnerHolder
import org.segment.kvctl.operator.KvrocksDBOperator
import org.segment.kvctl.operator.RedisDBOperator
import org.slf4j.LoggerFactory

def h = CommandTaskRunnerHolder.instance
def log = LoggerFactory.getLogger(this.getClass())

h.add('one shard node operate runner') { cmd ->
    '''
view_shard_node
check_shard_node
check_all_shard_node
fix_migrating_node
down_shard_node
up_shard_node
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

    if (cmd.hasOption('check_all_shard_node')) {
        shardDetail.iterateEach { shardNode ->
            def result = shardNode.statusCheck(shardDetail)
            if (result.isOk) {
                println 'OK cluster status/nodes/node id/slot range all is ok, ' + shardNode.uuid()
            } else {
                println 'Not ok, ' + shardNode.uuid() + ', message: ' + result.message
            }
        }
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

    if (!cmd.hasOption('down_shard_node')) {
        if (!App.isPortListening(port as int, ip)) {
            log.error 'target ip port is not listening'
            return
        }
    }

    if (cmd.hasOption('view_shard_node')) {
        def clusterNode = shardNode.clusterNode()
        println clusterNode.logPretty()
        return
    }

    if (cmd.hasOption('check_shard_node')) {
        def result = shardNode.statusCheck(shardDetail)
        if (result.isOk) {
            println 'OK cluster status/nodes/node id/slot range all is ok, ' + shardNode.uuid()
        } else {
            println 'Not ok, ' + shardNode.uuid() + ', message: ' + result.message
        }
        return
    }

    // deprecated
    if (cmd.hasOption('fix_migrating_node')) {
        def isEngineRedis = Conf.instance.isOn('app.engine.isRedis')
        if (isEngineRedis) {
            RedisDBOperator.refreshOneShardNodeWhenRestart(shardNode)
        } else {
            KvrocksDBOperator.refreshOneShardNodeWhenRestart(shardNode)
        }
        return
    }

    if (cmd.hasOption('down_shard_node')) {
        shardNode.isDown = true
        app.saveShardDetail()
        log.info 'done refresh local shard detail, -V for more'
        return
    }

    if (cmd.hasOption('up_shard_node')) {
        shardNode.isDown = false
        app.saveShardDetail()
        log.info 'done refresh local shard detail, -V for more'
        return
    }
}
