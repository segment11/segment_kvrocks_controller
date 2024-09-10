package org.segment.leaf

import groovy.sql.Sql
import groovy.util.logging.Slf4j
import org.h2.jdbcx.JdbcDataSource
import spock.lang.Specification

import java.util.concurrent.CountDownLatch

@Slf4j
class OneBizTest extends Specification {
    private Sql sql

    void setup() {
        var dataSource = new JdbcDataSource()
        dataSource.url = 'jdbc:h2:~/test-segment-leaf'
        dataSource.user = 'sa'
        dataSource.password = ''

        sql = new Sql(dataSource)

        String ddl = '''
create table if not exists leaf_alloc(
    biz_tag varchar(128) primary key,
    max_id bigint not null default '1',
    step int,
    description varchar(256) default null,
    update_time timestamp default current_timestamp
)
'''.trim()
        sql.execute(ddl)
        DsHolder.instance.dataSource = dataSource
    }

    void cleanup() {
        sql.executeUpdate('delete from leaf_alloc')
        log.info 'clear table leaf_alloc'
    }

    private void addOneRow() {
        String addSql = '''
insert into leaf_alloc(biz_tag, max_id, step, description) values('leaf-segment-test', 1, 2000, 'Test leaf Segment Mode Get Id')
'''
        sql.executeUpdate(addSql)
    }

    def 'test get all list'() {
        given:
        addOneRow()
        and:
        def r = OneBiz.getAllList()
        expect:
        r.size() == 1
        r[0].bizTag == 'leaf-segment-test'
    }

    def 'test update max id'() {
        given:
        addOneRow()
        and:
        def one = OneBiz.updateMaxId('leaf-segment-test')
        expect:
        one.maxId == 2001
    }

    def 'test update max id multi threads'() {
        given:
        addOneRow()
        and:
        int loopTimes = 100
        def set = Collections.synchronizedSortedSet(new TreeSet<Long>())
        def latch = new CountDownLatch(loopTimes)
        loopTimes.times {
            Thread.start {
                try {
                    set << OneBiz.updateMaxId('leaf-segment-test').maxId
                } finally {
                    latch.countDown()
                }
            }
        }
        latch.await()
        log.info 'first max id: ' + set[0]
        log.info 'middle max id: ' + set[(loopTimes / 2) as int]
        log.info 'last max id: ' + set[-1]
        expect:
        set.size() == 100
    }

    def 'test update max id by custom step'() {
        given:
        addOneRow()
        and:
        def one = OneBiz.updateMaxIdByCustomStep('leaf-segment-test', 10000)
        expect:
        one.maxId == 10001
    }

    def 'test get all biz tag list'() {
        given:
        addOneRow()
        and:
        def list = OneBiz.getAllBizTagList()
        expect:
        list[0] == 'leaf-segment-test'
    }
}
