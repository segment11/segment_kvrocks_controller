package org.segment.kvctl.check

import groovy.transform.CompileStatic

@CompileStatic
class StatusCheckResult {
    boolean isOk = true
    String message

    private StatusCheckResult(boolean isOk, String message) {
        this.isOk = isOk
        this.message = message
    }

    static StatusCheckResult fail(String message) {
        new StatusCheckResult(false, message)
    }

    static StatusCheckResult ok(String message = null) {
        new StatusCheckResult(true, message)
    }
}
