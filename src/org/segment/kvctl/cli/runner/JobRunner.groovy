package org.segment.kvctl.cli.runner

import org.segment.d.Record
import org.segment.kvctl.App
import org.segment.kvctl.cli.CommandTaskRunnerHolder
import org.segment.kvctl.cli.TablePrinter
import org.segment.kvctl.db.JobLogDTO
import org.segment.kvctl.db.MigrateTmpSaveDTO
import org.slf4j.LoggerFactory

def h = CommandTaskRunnerHolder.instance
def log = LoggerFactory.getLogger(this.getClass())

h.add('job runner') { cmd ->
    '''
job_log
delete_job_log
delete_tmp_saved_migrated_slot_log
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

            def one = new MigrateTmpSaveDTO(jobLogId: jid as int).queryFields('id').one()
            if (one) {
                new MigrateTmpSaveDTO(id: one.id).delete()
                log.info 'done delete tmp_saved_migrated_slot_log, job log id: {}', jid
            }
        }
        return
    }
}
