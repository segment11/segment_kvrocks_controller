package org.segment.kvctl.model

import groovy.transform.CompileStatic
import groovy.transform.ToString

@CompileStatic
@ToString(includeNames = true, includePackage = false)
class SlotNode {
    Integer beginSlot
    Integer endSlot
    String ip
    Integer port
    String nodeId
}
