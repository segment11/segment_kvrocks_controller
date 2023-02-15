package org.segment.kvctl.cli.runner

import org.segment.kvctl.App
import org.segment.kvctl.ClusterVersionHelper
import org.segment.kvctl.Conf
import org.segment.kvctl.cli.CommandTaskRunnerHolder
import org.segment.kvctl.cli.TablePrinter
import org.segment.kvctl.model.MultiSlotRange
import org.segment.kvctl.operator.RedisDBOperator
import org.segment.kvctl.shard.Shard
import org.segment.kvctl.shard.ShardDetail
import org.segment.kvctl.shard.ShardNode
import org.slf4j.LoggerFactory

def h = CommandTaskRunnerHolder.instance
def log = LoggerFactory.getLogger(this.getClass())

h.add('init batch') { cmd ->
    '''
init_batch
'''.readLines().collect { it.trim() }.findAll { it }.any {
        cmd.hasOption(it)
    }
} { cmd ->
    def app = App.instance
    if (app.shardDetail && app.shardDetail.shards) {
        log.warn 'shard exists, need clear before init batch'
        return
    }

    def numberPat = ~/^\d+$/

    def shardDetail = new ShardDetail()
    shardDetail.version = ClusterVersionHelper.instance.get(app.id)

    int shardIndex = 0

    // --init_batch=192.168.99.100:6379,192.168.99.100:6380
    def val = cmd.getOptionValue('init_batch')
    def arr = val.split(',')
    for (one in arr) {
        def arr2 = one.split(':')
        if (arr2.length != 2) {
            log.warn 'need ip:port, eg. -b=192.168.99.100:6379'
            return
        }

        def ip = arr2[0]
        def port = arr2[1]

        if (!port || !port.matches(numberPat)) {
            log.warn 'port must be a number'
            return
        }

        if (!App.isPortListening(port as int, ip)) {
            log.error 'target ip port is not listening'
            return
        }

        def shard = new Shard(shardIndex: shardIndex, multiSlotRange: new MultiSlotRange())
        shard.nodeList << new ShardNode(ip: ip, port: port as int, isPrimary: true, shardIndex: shardIndex)
        shardDetail.shards << shard

        shardIndex++
    }

    shardDetail.splitSlots()
    app.shardDetail = shardDetail
    app.saveShardDetail()

    def isEngineRedis = Conf.instance.isOn('app.engine.isRedis')
    if (isEngineRedis) {
        for (shard in shardDetail.shards) {
            for (shardNode in shard.nodeList) {
                shardDetail.iterateEach { oneShardNode ->
                    if (oneShardNode.uuid() == shardNode.uuid()) {
                        return
                    }
                    RedisDBOperator.meetNode(oneShardNode, shardNode)
                }
            }
        }
        log.info 'wait a while 2s'
        Thread.sleep(2000)
    }

    app.refreshAllShardNode()

    List<List<String>> table = shardDetail.logPretty()
    TablePrinter.print(table)
}
