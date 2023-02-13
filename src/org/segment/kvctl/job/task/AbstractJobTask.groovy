package org.segment.kvctl.job.task

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.segment.kvctl.App
import org.segment.kvctl.db.JobLogDTO
import org.segment.kvctl.job.JobResult

@CompileStatic
@Slf4j
abstract class AbstractJobTask {
    String step

    Integer jobLogId
    JobLogDTO jobLog

    AbstractJobTask(String step) {
        this.step = step
    }

    protected String stepAsUuid() {
        step
    }

    JobResult run() {
        def app = App.instance

        def one = new JobLogDTO(appId: app.id, step: stepAsUuid()).one()
        if (!one || one.step.contains(' expired')) {
            def newOne = new JobLogDTO()
            newOne.appId = app.id
            newOne.step = stepAsUuid()
            newOne.isOk = false
            newOne.createdDate = new Date()
            newOne.updatedDate = new Date()
            jobLog = newOne
            jobLogId = newOne.add()
        } else {
            // if already done
            if (one.isOk) {
                log.warn 'job log already done ok, job log id: {}, step: {}', one.id, stepAsUuid()
                return JobResult.ok()
            }
            jobLog = one
            jobLogId = one.id
        }
        log.info 'ready to do job log id: {}', jobLogId

        long beginT = System.currentTimeMillis()
        try {
            def result = doTask()
            long costMs = System.currentTimeMillis() - beginT
            def jobLog = new JobLogDTO(
                    id: jobLogId,
                    isOk: result.isOk,
                    message: result.message,
                    costMs: costMs.intValue(),
                    updatedDate: new Date())
            jobLog.update()

            return result
        } catch (Exception e) {
            long costMs = System.currentTimeMillis() - beginT
            log.error('job task error, app id: ' + app.id, ' step: ' + step, e)
            new JobLogDTO(
                    id: jobLogId,
                    isOk: false,
                    message: e.message,
                    costMs: costMs.intValue(),
                    updatedDate: new Date()).update()
            return JobResult.fail('step: ' + step + ' ex: ' + e.message)
        }
    }

    abstract JobResult doTask()
}
