package org.segment.kvctl

import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Options
import org.segment.d.D
import org.segment.d.Ds
import org.segment.d.MySQLDialect
import org.segment.kvctl.cli.CommandTaskRunnerHolder
import org.segment.kvctl.cli.hot.CachedGroovyClassLoader
import org.segment.kvctl.cli.hot.RefreshLoader
import org.segment.kvctl.jedis.JedisPoolHolder
import org.slf4j.LoggerFactory

def log = LoggerFactory.getLogger(this.getClass())

// project work directory set
String[] x = super.binding.getProperty('args') as String[]
def c = Conf.instance.resetWorkDir().load(x)
log.info c.toString()

// check if running in jar not groovy
if (new File(c.projectPath('/segment_kvrocks_controller-1.0.jar')).exists()) {
    c.on('cli.runtime.jar')
    log.info 'running in jar'
}

// init db access
def dbDataDir = c.getString('dbDataDir', '/opt/kvrocks_controller_data')
// not windows, do not use D: prefix
if (!System.getProperty('os.name').toLowerCase().contains('windows')) {
    dbDataDir = dbDataDir.replace('D:', '')
}
def ds = Ds.h2LocalWithPool(dbDataDir, 'default_ds')
def d = new D(ds, new MySQLDialect())
// check if need create table first
def tableNameList = d.query("show tables", String).collect { it.toUpperCase() }
if (!('APP' in tableNameList)) {
    def ddl = '''
create table app (
    id int auto_increment primary key,
    name varchar(100),
    password varchar(20),
    shard_detail mediumtext,
    updated_date timestamp default current_timestamp
);
create unique index idx_app_name on app(name);

create table job_log (
    id int auto_increment primary key,
    app_id int,
    is_ok bit,
    step varchar(200),
    message text,
    extend_params varchar(2000),
    cost_ms int,
    created_date timestamp,
    updated_date timestamp default current_timestamp
);
create index idx_job_log_app_id on job_log(app_id);

create table migrate_tmp_save (
    id int auto_increment primary key,
    app_id int,
    job_log_id int,
    type varchar(2),
    ip varchar(20),
    port int,
    slot_range_value text,
    updated_date timestamp default current_timestamp
);
create index idx_migrate_tmp_save_app_id on migrate_tmp_save(app_id);

create table leaf_alloc (
  biz_tag varchar(128)  NOT NULL DEFAULT '',
  max_id bigint(20) NOT NULL DEFAULT '1',
  step int(11) NOT NULL,
  description varchar(256)  DEFAULT NULL,
  update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (biz_tag)
);
'''
    ddl.trim().split(';').each {
        try {
            d.exe(it.toString())
        } catch (Exception e) {
            log.error('create table fail, ex: ' + e.message)
        }
    }
}

// cluster version increase using segment-leaf
ClusterVersionHelper.instance.init()

// groovy class loader init
def srcDirPath = c.projectPath('/src')
def resourceDirPath = c.projectPath('/resources')
def loader = CachedGroovyClassLoader.instance
loader.init(c.class.classLoader, srcDirPath + ':' + resourceDirPath)

// load command line runner dyn in target dir
def refreshLoader = RefreshLoader.create(loader.gcl).addClasspath(srcDirPath).addClasspath(resourceDirPath).
        addDir(c.projectPath('/src/org/segment/kvctl/cli/runner')).jarLoad(c.isOn('cli.runtime.jar'))
refreshLoader.refresh()
refreshLoader.start()

def options = new Options()
options.addOption('n', 'name', true,
        'define a unique application name for target kvrocks cluster, eg. myCluster1 or testCluster')
options.addOption('p', 'password', true, 'kvrocks server password')
options.addOption('C', 'clear', false, 'clear old application saved local')
options.addOption('I', 'import', true,
        'import new application from exist kvrocks cluster, require ip:port, eg. -I=192.168.99.100:6379')
options.addOption('S', 'shard_slot_range_re_avg', true,
        'migrate other slot to target primary node so this shard slot range will be avg, ' +
                'require ip:port, eg. -S=192.168.99.100:6379')

options.addOption('v', 'view_shard_node', false, 'view one shard node cluster info/nodes/slots')
options.addOption('V', 'view_shard_detail', false, 'view all shard nodes')
options.addOption('c', 'check_shard_node', false, 'check one shard node cluster info/nodes/slots if ok and match')
options.addOption('A', 'check_all_shard_node', false, 'check all shard nodes cluster info/nodes/slots if ok and match')

options.addOption('a', 'meet_node', false, 'operation: add one shard node to cluster')
options.addOption('d', 'forget_node', false, 'operation: remove one shard node from cluster')
options.addOption('f', 'failover', false, 'operation: failover one shard replica node as primary')

options.addOption('h', 'ip', true, 'target node ip')
options.addOption('P', 'port', true, 'target node port')
options.addOption('s', 'shard_index', true, 'target shard index, eg. 0 or 1')
options.addOption('m', 'is_primary', false, 'is primary, eg. true or false')
options.addOption('r', 'replica_index', true, 'target replica index, eg. 0 or 1')

options.addOption('J', 'job_log', false, 'display job log')
options.addOption('L', 'delete_job_log', true, 'delete job log by id, eg. -L=* or -L=1,2,3')
options.addOption('U', 'tmp_saved_migrated_slot_log', false, 'display tmp saved migrated slot log')
options.addOption('T', 'delete_tmp_saved_migrated_slot_log', true, 'delete by job log id, eg. -T=* or -T=1,2,3')
options.addOption('R', 'redo_one_job_by_id', true, 'redo one job by one job log id, eg. -R=1')
options.addOption('O', 'redo_one_job_by_id_force', false,
        'redo one job by one job log id ignore done and result is ok, eg. -R=1 -O')

options.addOption('F', 'migrate_slots', true, 'migrate some slots to current session ip/port shard node, eg. -F=0')
options.addOption('M', 'fix_migrating_node', false, 'fix refresh cluster nodes after restart when migrate job undone')
options.addOption('N', 'down_shard_node', false, 'set target shard node down')
options.addOption('Y', 'up_shard_node', false, 'set target shard node up')

options.addOption('D', 'delete', false, 'delete application local')
options.addOption('H', 'help', false, 'args help')
options.addOption('Q', 'quit', false, 'quit console')
options.addOption('X', 'x_session_current_variables', false, 'view args for reuse in current session')

def formatter = new HelpFormatter()
formatter.printHelp('please input follow args to run task', options)
println '----- begin console interact -----'
println 'input .. to reuse latest line input'

def isLocalTest = c.isOn('isLocalTest')

String globalName = isLocalTest ? 'test' : c.get('app.globalName')
String globalPassword = isLocalTest ? 'test1234' : c.get('app.globalPassword')
String globalIp
String globalPort

String lastLine

def parser = new DefaultParser()

def br = new BufferedReader(new InputStreamReader(System.in))
while (true) {
    def line = br.readLine().trim()

    if (line == 'quit') {
        println 'quit from console...'
        break
    } else if (line == 'help') {
        println 'java -jar segment_kvrocks_controller-1.0.jar'
        println 'you mean --help?'
        continue
    } else if (line.startsWith('..') || line.startsWith('-') || line.startsWith('--')) {
        def lineArgs = line.split(' ')
        boolean needReplaceLastLine = false
        for (int i = 0; i < lineArgs.length; i++) {
            def arg = lineArgs[i]
            if ('..' == arg) {
                lineArgs[i] = lastLine
                needReplaceLastLine = true
            }
        }

        String finalLine
        if (needReplaceLastLine) {
            finalLine = lineArgs.join(' ')
            println finalLine
        } else {
            finalLine = line
        }

        if (globalName && !finalLine.contains('-n=') && !finalLine.contains('--name=')) {
            finalLine += (' -n=' + globalName)
        }
        if (globalPassword && !finalLine.contains('-p=') && !finalLine.contains('--password=')) {
            finalLine += (' -p=' + globalPassword)
        }
        if (globalIp && !finalLine.contains('-h=') && !finalLine.contains('--ip=')) {
            finalLine += (' -h=' + globalIp)
        }
        if (globalPort && !finalLine.contains('-P=') && !finalLine.contains('--port=')) {
            finalLine += (' -P=' + globalPort)
        }

        lastLine = finalLine
        CommandLine cmd
        try {
            cmd = parser.parse(options, finalLine.split(' '))

            if (cmd.hasOption('quit')) {
                println 'quit...'
                break
            }

            if (cmd.hasOption('help')) {
                formatter.printHelp('please input follow args to run task', options)
                println '----- begin console interact -----'
                println 'input .. to reuse latest line input'
                continue
            }
        } catch (Exception e) {
            log.error 'args parse fail, input help for more information, ex: ' + e.message
            continue
        }

        if (cmd.hasOption('name')) {
            globalName = cmd.getOptionValue('name')
        }

        if (cmd.hasOption('password')) {
            globalPassword = cmd.getOptionValue('password')
        }

        if (cmd.hasOption('ip')) {
            globalIp = cmd.getOptionValue('ip')
        }

        if (cmd.hasOption('port')) {
            globalPort = cmd.getOptionValue('port')
        }

        if (cmd.hasOption('x_session_current_variables')) {
            println 'app id: '.padRight(20, ' ') + App.instance.id
            println 'name: '.padRight(20, ' ') + globalName
            println 'password: '.padRight(20, ' ') + globalPassword
            println 'ip: '.padRight(20, ' ') + globalIp
            println 'port: '.padRight(20, ' ') + globalPort
            continue
        }

        if (!cmd.hasOption('name') || !cmd.hasOption('password')) {
            log.error 'name and password required'
            continue
        }

        try {
            def app = App.instance
            app.init(cmd.getOptionValue('name'), cmd.getOptionValue('password'), cmd.hasOption('clear'))

            def isDone = CommandTaskRunnerHolder.instance.run(cmd)
            if (!isDone) {
                println '--help for valid args'
            }
        } catch (IllegalStateException e) {
            log.warn e.message
        } catch (Exception e) {
            log.error 'run task error', e
        }
    }
}

refreshLoader.stop()
JedisPoolHolder.instance.close()
ClusterVersionHelper.instance.close()
Ds.disconnectAll()
