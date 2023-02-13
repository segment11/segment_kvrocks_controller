package org.segment.kvctl.db

import groovy.transform.CompileStatic
import groovy.transform.ToString
import org.segment.kvctl.db.ext.ExtendParams

@CompileStatic
@ToString(includeNames = true, includeSuper = false)
class JobLogDTO extends BaseRecord<JobLogDTO> {
    Integer id

    Integer appId

    String step

    String message

    Boolean isOk

    ExtendParams extendParams

    Integer costMs

    Date createdDate

    Date updatedDate

    String param(String key) {
        extendParams?.get(key)
    }

    JobLogDTO addParam(String key, String value) {
        if (extendParams == null) {
            extendParams = new ExtendParams()
        }
        extendParams.put(key, value)
        this
    }
}