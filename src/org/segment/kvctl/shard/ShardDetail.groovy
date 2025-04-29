package org.segment.kvctl.shard

import groovy.transform.CompileStatic
import org.segment.d.json.JSONFiled
import org.segment.kvctl.App
import org.segment.kvctl.ClusterVersionHelper

@CompileStatic
class ShardDetail implements JSONFiled {
    List<Shard> shards = []

    List<Shard> shardsLastClusterVersion = []

    Long version

    Shard oneShard(Integer shardIndex) {
        shards.find { it.shardIndex == shardIndex as int }
    }

    Integer maxShardIndex() {
        if (!shards) {
            return -1
        }
        shards.collect { it.shardIndex }.max()
    }

    @Override
    String toString() {
        if (!shards) {
            return ''
        }

        List<String> r = []
        iterateEach {
            r << it.toString()
        }
        r.join('\r\n')
    }

    void saveLastVersion(Closure closure) {
        shardsLastClusterVersion = shards.collect {
            it.clone()
        }
        version = ClusterVersionHelper.instance.get(App.instance.id)

        try {
            closure()
        } catch (Exception e) {
            // may be problem, eg. add a shard primary node, migrated some slots, crash down
            // need save to db for next time load
            restoreLastVersion()
            throw e
        }
    }

    void restoreLastVersion() {
        shards = shardsLastClusterVersion.collect {
            it.clone()
        }
        version = ClusterVersionHelper.instance.get(App.instance.id)
    }

    ShardDetail deepClone() {
        def c = new ShardDetail()
        c.shards = []
        c.shardsLastClusterVersion = []
        shards.each {
            c.shards << it.clone()
        }
        if (shardsLastClusterVersion) {
            shardsLastClusterVersion.each {
                c.shardsLastClusterVersion << it.clone()
            }
        }
        c
    }

    void splitSlots() {
        int pageNum = 1
        for (shard in shards) {
            def pager = SlotBalancer.splitAvg(shards.size(), pageNum)
            pageNum++

            shard.multiSlotRange.list.clear()
            shard.multiSlotRange.addSingle(pager.start, pager.end - 1)
        }
    }

    List<String> clusterNodesArgsLastVersion() {
        clusterNodesArgs(null, null, null, shardsLastClusterVersion)
    }

    List<String> clusterNodesArgs(ShardNode oneShardNode = null,
                                  TreeSet<Integer> migratedSlotSet = null, TreeSet<Integer> importedSlotSet = null,
                                  List<Shard> targetShards = null) {
        // topology
        List<String> argsList = []
        for (shard in (targetShards ?: shards)) {
            for (shardNode in shard.nodeList) {
                def nodeId = shard.nodeId(shardNode)
                def ip = shardNode.ip
                def port = shardNode.port

                if (shardNode.isPrimary) {
                    if (!oneShardNode || oneShardNode.ip != ip) {
                        // target one shard node already imported these slots, this shard node should exclude these slots
                        def multiSlotRange = shard.multiSlotRange.removeSet(importedSlotSet)
                        argsList.addAll(multiSlotRange.clusterNodesArgs(nodeId, ip, port))
                    } else {
                        def multiSlotRange = shard.multiSlotRange.removeSet(migratedSlotSet, importedSlotSet)
                        argsList.addAll(multiSlotRange.clusterNodesArgs(nodeId, ip, port))
                    }
                } else {
                    argsList << "${nodeId} ${ip} ${port} slave ${shard.primaryNodeId()}".toString()
                }
            }
        }
        argsList
    }

    @CompileStatic
    static interface ShardNodeMatcher {
        boolean isMatch(ShardNode shardNode)
    }

    List<String> ipsWithMatcher(ShardNodeMatcher matcher = null) {
        List<String> list = []
        if (!shards) {
            return list
        }

        for (shard in shards) {
            if (!shard.nodeList) {
                continue
            }
            for (shardNode in shard.nodeList) {
                if (matcher == null || matcher.isMatch(shardNode)) {
                    list.add(shardNode.ip)
                }
            }
        }
        list
    }

    List<String> ips() {
        ipsWithMatcher()
    }

    Shard findShardByIpPort(String ip, Integer port) {
        if (!shards) {
            return null
        }

        for (shard in shards) {
            for (shardNode in shard.nodeList) {
                if (shardNode.ip == ip && shardNode.port == port) {
                    return shard
                }
            }
        }

        for (shard in shardsLastClusterVersion) {
            for (shardNode in shard.nodeList) {
                if (shardNode.ip == ip && shardNode.port == port) {
                    return shard
                }
            }
        }
        null
    }

    ShardNode findShardNodeByIpPort(String ip, Integer port, boolean isIncludeLastVersion = true) {
        if (!shards) {
            return null
        }
        for (shard in shards) {
            for (shardNode in shard.nodeList) {
                if (shardNode.ip == ip && shardNode.port == port) {
                    return shardNode
                }
            }
        }

        if (isIncludeLastVersion) {
            for (shard in shardsLastClusterVersion) {
                for (shardNode in shard.nodeList) {
                    if (shardNode.ip == ip && shardNode.port == port) {
                        return shardNode
                    }
                }
            }
        }
        null
    }

    void iterateEach(ShardNodeCallback callback) {
        if (!shards) {
            return
        }
        for (shard in shards) {
            for (shardNode in shard.nodeList) {
                callback(shardNode)
            }
        }
    }

    List<List<String>> logPretty() {
        List<List<String>> r = []
        List<String> header = ['Shard Index', 'IP', 'Port', 'Is Primary', 'Replica Index', 'Is Down',
                               'Begin Slot', 'End Slot', 'Total Slot Number']
        r << header

        for (it in shards) {
            r.addAll it.logPretty()
        }
        r
    }
}
