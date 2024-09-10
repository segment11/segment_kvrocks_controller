package org.segment.kvctl.model

import groovy.transform.CompileStatic
import groovy.transform.ToString
import groovy.util.logging.Slf4j
import org.segment.kvctl.Conf
import org.segment.kvctl.ex.JobHandleException
import org.segment.kvctl.jedis.ClusterSetCommand
import org.segment.kvctl.jedis.JedisPoolHolder
import org.segment.kvctl.jedis.MessageReader
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisClusterException

@CompileStatic
@Slf4j
@ToString(includeNames = true, includePackage = false,
        excludes = ['multiSlotRange', 'clusterInfoKVList', 'allClusterNodeList', 'allSlotNodeList'])
class ClusterNode {
    String ip
    Integer port

    ClusterNode() {}

    ClusterNode(String ip, Integer port) {
        this.ip = ip
        this.port = port
    }

    String uuid() {
        ip + ':' + port
    }

    String nodeId
    Boolean isPrimary
    String followNodeId
    // for easy check
    String followNodeIp
    Integer followNodePort
    Boolean isMySelf

    // only myself use by ShardNode.clusterNode() has values, allClusterNodeList only has above
    MultiSlotRange multiSlotRange
    List<KVPair> clusterInfoKVList

    List<ClusterNode> allClusterNodeList
    List<SlotNode> allSlotNodeList

    String logPretty() {
        def sb = new StringBuilder()
        sb << '--- base ---'
        sb << '\n'
        sb << this.toString()
        sb << '\n'
        sb << '--- end ---'
        sb << '\n'
        sb << '--- cluster info ---'
        sb << '\n'
        clusterInfoKVList.each {
            sb << it.toString()
            sb << '\n'
        }
        sb << '--- end ---'
        sb << '\n'
        sb << '--- slots ---'
        sb << '\n'
        multiSlotRange.list.each {
            sb << it.toString()
            sb << '\n'
        }
        sb << '--- end ---'
        sb << '\n'
        sb << '--- other nodes ---'
        sb << '\n'
        for (clusterNode in allClusterNodeList) {
            if (clusterNode.nodeId != nodeId) {
                sb << clusterNode.toString()
                sb << '\n'
            }
        }
        sb << '--- end ---'
        sb << '\n'
        sb << '--- other node slots ---'
        sb << '\n'
        for (slotNode in allSlotNodeList) {
            if (slotNode.nodeId != nodeId) {
                sb << slotNode.toString()
                sb << '\n'
            }
        }
        sb << '--- end ---'
        sb.toString()
    }

    String clusterInfoValue(String key) {
        clusterInfoKVList?.find { it.key == key }?.value
    }

    Long clusterVersion() {
        clusterInfoValue('cluster_my_epoch') as Long
    }

    String clusterState() {
        clusterInfoValue('cluster_state')
    }

    private void setClusterNodeId(Jedis jedis, String nodeId) {
        def command = new ClusterSetCommand("CLUSTERX")
        byte[] r = jedis.sendCommand(command, "SETNODEID".bytes, "${nodeId}".toString().bytes) as byte[]
        def result = new String(r)
        log.info 'set node id {} result: {}, this node: {}', nodeId, result, uuid()
        if ('OK' != result) {
            throw new JobHandleException('set node id fail, result: ' + result + ', this node: ' + uuid())
        }
    }

    boolean init(String nodeId) {
        def isEngineRedis = Conf.instance.isOn('app.engine.isRedis')
        if (isEngineRedis) {
            return true
        }

        def isEngineVelo = Conf.instance.isOn('app.engine.isVelo')

        def jedisPool = JedisPoolHolder.instance.create(ip, port)
        JedisPoolHolder.useRedisPool(jedisPool) { jedis ->
            try {
                if (isEngineVelo) {
                    setClusterNodeId(jedis, nodeId)
                } else {
                    def clusterNodes = jedis.clusterNodes()
                    def hasMySelf = clusterNodes.contains('myself')
                    if (!hasMySelf) {
                        setClusterNodeId(jedis, nodeId)
                    }
                }
                true
            } catch (JedisClusterException e) {
                // cluster nodes not set yet
                setClusterNodeId(jedis, nodeId)
                false
            }
        }
    }

    ClusterNode read() {
        assert ip && port

        def that = this
        def jedisPool = JedisPoolHolder.instance.create(ip, port)
        JedisPoolHolder.useRedisPool(jedisPool) { jedis ->
            def allClusterNodeList = MessageReader.fromClusterNodes(jedis.clusterNodes(), uuid())

            def myNode = allClusterNodeList.find { it.isMySelf }
            // why happen ?
            if (myNode == null) {
                throw new IllegalStateException('cluster nodes not include myself. this node: ' + uuid() +
                        ', cluster nodes: ' + allClusterNodeList)
            }

            that.nodeId = myNode.nodeId
            that.isPrimary = myNode.isPrimary
            that.followNodeId = myNode.followNodeId
            that.followNodeIp = myNode.followNodeIp
            that.followNodePort = myNode.followNodePort
            that.isMySelf = myNode.isMySelf

            that.allClusterNodeList = allClusterNodeList

            def allSlotNodeList = MessageReader.fromClusterSlots(jedis.clusterSlots())
            that.allSlotNodeList = allSlotNodeList

            def mySlotNodeList = allSlotNodeList.findAll { it.nodeId == myNode.nodeId }
            def multiSlotRange = new MultiSlotRange()
            for (slotNode in mySlotNodeList) {
                multiSlotRange.addSinge(slotNode.beginSlot, slotNode.endSlot)
            }
            that.multiSlotRange = multiSlotRange

            that.clusterInfoKVList = MessageReader.fromClusterInfo(jedis.clusterInfo())
        }
        that
    }
}
