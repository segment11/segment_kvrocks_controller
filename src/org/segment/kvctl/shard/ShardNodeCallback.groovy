package org.segment.kvctl.shard

import groovy.transform.CompileStatic

@CompileStatic
interface ShardNodeCallback {
    void call(ShardNode shardNode)
}
