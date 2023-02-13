package org.segment.kvctl.model

import groovy.transform.CompileStatic
import groovy.transform.TupleConstructor

@CompileStatic
@TupleConstructor
class KVPair {
    String key
    String value

    @Override
    String toString() {
        "${key}=${value}".toString()
    }
}
