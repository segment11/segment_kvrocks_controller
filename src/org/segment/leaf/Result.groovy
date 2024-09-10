package org.segment.leaf

import groovy.transform.CompileStatic

@CompileStatic
class Result {
    long id
    Status status

    Result(long id, Status status) {
        this.id = id
        this.status = status
    }

    @CompileStatic
    static enum Status {
        SUCCESS, EXCEPTION
    }

    static final Result ZERO = new Result(0, Status.SUCCESS)

    @Override
    String toString() {
        "Result:id=${id},status=${status}"
    }
}
