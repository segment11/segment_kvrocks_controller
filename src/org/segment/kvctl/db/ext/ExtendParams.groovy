package org.segment.kvctl.db.ext

import groovy.transform.CompileStatic
import groovy.transform.ToString
import org.segment.d.json.JSONFiled

@CompileStatic
@ToString(includeNames = true, includePackage = false)
class ExtendParams implements JSONFiled {
    Map<String, String> params = [:]

    String get(String key) {
        if (!params) {
            return null
        }
        params[key]
    }

    void put(String key, String value) {
        params[key] = value
    }

    boolean asBoolean() {
        params != null && params.size() > 0
    }
}
