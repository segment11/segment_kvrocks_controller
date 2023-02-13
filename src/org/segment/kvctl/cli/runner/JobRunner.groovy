package org.segment.kvctl.cli.runner

import org.segment.d.Record
import org.segment.kvctl.App
import org.segment.kvctl.cli.CommandTaskRunnerHolder
import org.segment.kvctl.cli.TablePrinter
import org.segment.kvctl.db.JobLogDTO
import org.segment.kvctl.db.MigrateTmpSaveDTO
import org.segment.kvctl.ex.JobHandleException
import org.segment.kvctl.job.task.MigrateSlotsJobTask
import org.slf4j.LoggerFactory

def h = CommandTaskRunnerHolder.instance
def log = LoggerFactory.getLogger(this.getClass())

h.add('job runner') { cmd ->
    '''
job_log
delete_job_log
tmp_saved_migrated_slot_log
delete_tmp_saved_migrated_slot_log
redo_one_job_by_id
'''.readLines().collect { it.trim() }.findAll { it }.any {
        cmd.hasOption(it)
    }
} { cmd ->
    def app = App.instance

    def numberPat = ~/^\d+$/

    if (cmd.hasOption('job_log')) {
        def jobLogList = new JobLogDTO(appId: app.id).loadList()
        if (jobLogList) {
            TablePrinter.printRecordList(jobLogList.collect { (Record) it })
        } else {
            log.warn 'no job log found'
        }
        return
    }

    if (cmd.hasOption('delete_job_log')) {
        def jobLogId = cmd.getOptionValue('delete_job_log')

        if (jobLogId == '*') {
            def n = new JobLogDTO(appId: app.id).deleteAll()
            log.info 'delete all job log for this app id: {}, delete rows: {}', app.id, n
            return
        }

        for (jid in jobLogId.split(',')) {
            if (!jid.matches(numberPat)) {
                log.warn 'job log id must be a number: {}', jid
                continue
            }

            new JobLogDTO(id: jid as int).delete()
            log.info 'done delete job log, job log id: {}', jid
        }
        return
    }

    if (cmd.hasOption('tmp_saved_migrated_slot_log')) {
        def list = new MigrateTmpSaveDTO(appId: app.id).loadList()
        if (list) {
            TablePrinter.printRecordList(list.collect { (Record) it })
        } else {
            log.warn 'no tmp saved migrated slot log found'
        }
        return
    }

    if (cmd.hasOption('delete_tmp_saved_migrated_slot_log')) {
        def jobLogId = cmd.getOptionValue('delete_tmp_saved_migrated_slot_log')

        if (jobLogId == '*') {
            def n = new MigrateTmpSaveDTO(appId: app.id).deleteAll()
            log.info 'delete all tmp_saved_migrated_slot_log for this app id: {}, delete rows: {}', app.id, n
            return
        }

        for (jid in jobLogId.split(',')) {
            if (!jid.matches(numberPat)) {
                log.warn 'job log id must be a number: {}', jid
                continue
            }

            def list = new MigrateTmpSaveDTO(jobLogId: jid as int).queryFields('id').loadList()
            if (list) {
                for (one in list) {
                    new MigrateTmpSaveDTO(id: one.id).delete()
                }
                log.info 'done delete tmp_saved_migrated_slot_log, job log id: {}, rows: {}', jid, list.size()
            } else {
                log.warn 'no tmp saved migrated slot log found by given job log id: {}', jid
            }
        }
        return
    }

    if (cmd.hasOption('redo_one_job_by_id')) {
        def jobLogId = cmd.getOptionValue('redo_one_job_by_id')
        if (!jobLogId.matches(numberPat)) {
            log.warn 'job log id must be a number: {}', jobLogId
            return
        }

        def jobLog = new JobLogDTO(id: jobLogId as int).one()
        if (!jobLog) {
            log.warn 'no jog log found'
            return
        }

        if (jobLog.isOk || jobLog.step.contains(' expired')) {
            if (!cmd.hasOption('redo_one_job_by_id_force')) {
                log.warn 'this job log is already done ok'
                return
            } else {
                new JobLogDTO(id: jobLog.id, isOk: false, message: 'redo force', updatedDate: new Date()).update()
            }
        } else {
            new JobLogDTO(id: jobLog.id, message: 'redo', updatedDate: new Date()).update()
        }

        def jsonString = jobLog.param(MigrateSlotsJobTask.PARAM_KEY_MYSELF)
        def task = MigrateSlotsJobTask.reloadFromJson(jsonString)

        def result = task.run()
        if (!result.isOk) {
            throw new JobHandleException('migrate slots job task run failed, abort - ' + task.stepAsUuid())
        }

        def shardDetail = App.instance.shardDetail

        // update from and to shard slot range
        def toShard = shardDetail.findShardByIpPort(task.toIp, task.toPort)
        toShard.multiSlotRange.addMerge(task.beginSlot, task.endSlot)

        def fromShard = shardDetail.findShardByIpPort(task.fromIp, task.fromPort)
        fromShard.multiSlotRange.removeMerge(task.beginSlot, task.endSlot)

        app.saveShardDetail()

        log.info 'done sub task: {}', task.stepAsUuid()

        List<List<String>> table = shardDetail.logPretty()
        TablePrinter.print(table)

        return
    }
}
