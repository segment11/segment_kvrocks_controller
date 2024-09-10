package org.segment.kvctl.check

import spock.lang.Specification

class StatusCheckResultTest extends Specification {
    def 'test base'() {
        given:
        def result = StatusCheckResult.ok()
        def failResult = StatusCheckResult.fail('fail')

        expect:
        result.isOk
        !failResult.isOk
        failResult.message == 'fail'
    }
}
