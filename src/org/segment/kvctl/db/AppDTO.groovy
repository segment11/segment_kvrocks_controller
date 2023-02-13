package org.segment.kvctl.db

import groovy.transform.CompileStatic
import groovy.transform.ToString
import org.segment.kvctl.shard.ShardDetail

@CompileStatic
@ToString(includeNames = true, includeSuper = false)
class AppDTO extends BaseRecord<AppDTO> {

    Integer id

    String name

    String password

    ShardDetail shardDetail

    Date updatedDate
}