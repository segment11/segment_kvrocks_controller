package org.segment.kvctl.cli.runner

import org.segment.kvctl.App
import org.segment.kvctl.cli.CommandTaskRunnerHolder
import org.slf4j.LoggerFactory

def h = CommandTaskRunnerHolder.instance
def log = LoggerFactory.getLogger(this.getClass())

h.add('failover runner') { cmd ->
    '''
failover
'''.readLines().collect { it.trim() }.findAll { it }.any {
        cmd.hasOption(it)
    }
} { cmd ->
    def app = App.instance

    def numberPat = ~/^\d+$/

    def shardIndex = cmd.getOptionValue('shard_index')
    def replicaIndex = cmd.getOptionValue('replica_index')
    if (!shardIndex || !replicaIndex) {
        log.warn 'shard_index and replica_index required'
        return
    }

    if (!shardIndex.matches(numberPat)) {
        log.warn 'shard index must be a number'
        return
    }

    if (!replicaIndex.matches(numberPat)) {
        log.warn 'replica index must be a number'
        return
    }

    app.failover(shardIndex as int, replicaIndex as int)
    println 'input -A for check all shard nodes cluster info/nodes/slots if ok and match'
}
