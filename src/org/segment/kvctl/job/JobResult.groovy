package org.segment.kvctl.job

import groovy.transform.CompileStatic

@CompileStatic
class JobResult {
    boolean isOk = true
    String message

    private JobResult(boolean isOk, String message) {
        this.isOk = isOk
        this.message = message
    }

    static JobResult fail(String message) {
        new JobResult(false, message)
    }

    static JobResult ok(String message = null) {
        new JobResult(true, message)
    }
}
