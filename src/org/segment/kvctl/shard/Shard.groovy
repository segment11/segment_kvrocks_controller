package org.segment.kvctl.shard

import groovy.transform.AutoClone
import groovy.transform.CompileStatic
import groovy.transform.ToString
import org.segment.kvctl.Conf
import org.segment.kvctl.model.MultiSlotRange
import org.segment.kvctl.operator.RedisDBOperator

@CompileStatic
@ToString(includeNames = true, includePackage = false)
@AutoClone
class Shard {
    Integer shardIndex

    MultiSlotRange multiSlotRange

    List<ShardNode> nodeList = []

    TreeSet<Integer> slotTreeSet() {
        multiSlotRange.toTreeSet()
    }

    HashSet<Integer> slotHashSet() {
        multiSlotRange.toHashSet()
    }

    ShardNode primary() {
        nodeList.find { it.isPrimary }
    }

    ShardNode replica(Integer replicaIndex) {
        nodeList.find { !it.isPrimary && it.replicaIndex == replicaIndex }
    }

    // generate a 40 char length node id for cluster set nodes
    // kvrocks need, redis use cluster my id get node id
    String nodeId(ShardNode shardNode) {
        def isEngineRedis = Conf.instance.isOn('app.engine.isRedis')
        if (isEngineRedis) {
            return RedisDBOperator.getNodeId(shardNode.ip, shardNode.port)
        }

        // for import cluster
        if (shardNode.nodeIdFix) {
            return shardNode.nodeIdFix
        }

        String prefix = 's' + shardIndex + 'ip' + shardNode.ip.replaceAll(/\./, 'x') +
                shardNode.port

        final int len = 40
        final int moreNumber = len - prefix.length() - 1

        prefix + 'x' + (0..<moreNumber).collect { 'a' }.join('')
    }

    String primaryNodeId() {
        nodeId(nodeList.find { it.isPrimary })
    }

    List<List<String>> logPretty() {
        List<List<String>> r = []
        for (it in nodeList) {
            def pretty = it.logPretty()
            if (multiSlotRange) {
                pretty << multiSlotRange.list.collect { it.begin }.toString()
                pretty << multiSlotRange.list.collect { it.end }.toString()
                pretty << multiSlotRange.totalNumber().toString()
            } else {
                pretty << ''
                pretty << ''
                pretty << ''
            }
            r << pretty
        }
        r
    }
}
