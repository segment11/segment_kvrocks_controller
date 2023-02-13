package org.segment.kvctl.db

import groovy.transform.CompileStatic
import groovy.transform.ToString

@CompileStatic
@ToString(includeNames = true, includeSuper = false)
class MigrateTmpSaveDTO extends BaseRecord<MigrateTmpSaveDTO> {

    Integer id

    Integer appId

    Integer jobLogId

    String type

    String ip

    Integer port

    String slotRangeValue

    Date updatedDate
}