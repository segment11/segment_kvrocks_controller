package org.segment.kvctl.cli.runner

import org.segment.kvctl.App
import org.segment.kvctl.cli.CommandTaskRunnerHolder
import org.segment.kvctl.cli.TablePrinter
import org.slf4j.LoggerFactory

def h = CommandTaskRunnerHolder.instance
def log = LoggerFactory.getLogger(this.getClass())

h.add('simple runner') { cmd ->
    '''
view_shard_detail
delete
'''.readLines().collect { it.trim() }.findAll { it }.any {
        cmd.hasOption(it)
    }
} { cmd ->
    def app = App.instance
    def shardDetail = app.shardDetail
    if (!shardDetail) {
        log.warn 'no shard exists'
        return
    }

    if (cmd.hasOption('view_shard_detail')) {
        List<List<String>> table = app.shardDetail.logPretty()
        TablePrinter.print(table)
        return
    }

    if (cmd.hasOption('delete')) {
        app.clearAll()
        log.warn 'application clear all, app id: {}', app.id
        return
    }
}
