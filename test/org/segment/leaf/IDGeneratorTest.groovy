package org.segment.leaf

import groovy.sql.Sql
import groovy.util.logging.Slf4j
import org.h2.jdbcx.JdbcDataSource
import org.segment.kvctl.Conf
import spock.lang.Specification

import javax.sql.DataSource
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

@Slf4j
class IDGeneratorTest extends Specification {
    DataSource dataSource

    void setup() {
        dataSource = new JdbcDataSource()
        dataSource.url = 'jdbc:h2:~/test-segment-leaf'
        dataSource.user = 'sa'
        dataSource.password = ''

        def sql = new Sql(dataSource)

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
        sql.executeUpdate('delete from leaf_alloc')
        // begin step = 100 so it can expand
        String addSql = '''
insert into leaf_alloc(biz_tag, max_id, step, description) values('leaf-segment-test', 1, 100, 'Test leaf Segment Mode Get Id')
'''
        sql.executeUpdate(addSql)
    }

    def 'test get'() {
        given:
        def id = new IDGenerator()
        id.dataSource = dataSource
        id.init()
        and:
        List<Result> list = []
        int loopTimes = 200
        loopTimes.times {
            list << id.get('leaf-segment-test')
            // mock do business
            long ms = 10 + new Random().nextInt(10)
            Thread.sleep(ms)
        }
        log.warn 'first result: ' + list[0].id
        log.warn 'middle result: ' + list[(loopTimes / 2) as int].id
        log.warn 'last result: ' + list[-1].id
        expect:
        list.size() == loopTimes
        list.every { it.status == Result.Status.SUCCESS }
        cleanup:
        id.close()
    }

    def 'test get multi threads'() {
        given:
        def id = new IDGenerator()
        id.name = 'id generator'
        id.dataSource = dataSource
        id.init()

        Conf.instance.put('wait_ms_when_next_segment_not_ready', '100')
        and:
        // set step bigger as segment may be not ready for many times
        def sql = new Sql(dataSource)
        sql.executeUpdate('update leaf_alloc set step = 2000')

        final int threadNumber = 100
        int loopTimes = 1000
        def set = Collections.synchronizedSortedSet(new TreeSet<Long>())
        def latch = new CountDownLatch(threadNumber)
        AtomicInteger failedNumber = new AtomicInteger(0)
        threadNumber.times {
            Thread.start {
                try {
                    loopTimes.times {
                        def idResult = id.get('leaf-segment-test').id
                        if (idResult < 0) {
                            failedNumber.incrementAndGet()
                        }
                        set << idResult
                    }
                } finally {
                    latch.countDown()
                }
            }
        }
        latch.await()
        def namePre = id.name.padRight(20, ' ') + 'first result: '
        log.warn namePre + set[0]
        log.warn namePre + 'middle result: ' + set[loopTimes * threadNumber / 2 as int]
        log.warn namePre + 'last result: ' + set[-1]
        log.warn namePre + 'failed number: ' + failedNumber
        expect:
        // get -3 many times
        set.size() <= loopTimes * threadNumber
        // first is -3
        set.size() + failedNumber - 1 == loopTimes * threadNumber
        // no exception
        set.every { it > 0 || it == -3 }
        cleanup:
        id.close()
        log.warn namePre + ' test done'
    }

    def 'test get multi threads multi instances'() {
        given:
        def id1 = new IDGenerator()
        id1.name = 'id generator 1'
        def id2 = new IDGenerator()
        id2.name = 'id generator 2'
        def id3 = new IDGenerator()
        id3.name = 'id generator 3'

        id1.dataSource = dataSource
        id2.dataSource = dataSource
        id3.dataSource = dataSource
        id1.init()
        id2.init()
        id3.init()

        Conf.instance.put('wait_ms_when_next_segment_not_ready', '100')
        and:
        // set step bigger as segment may be not ready for many times
        def sql = new Sql(dataSource)
        sql.executeUpdate('update leaf_alloc set step = 10000')

        final int threadNumber = 100
        int loopTimes = 1000
        def set1 = Collections.synchronizedSortedSet(new TreeSet<Long>())
        def set2 = Collections.synchronizedSortedSet(new TreeSet<Long>())
        def set3 = Collections.synchronizedSortedSet(new TreeSet<Long>())
        def latch = new CountDownLatch(threadNumber)
        AtomicInteger failedNumber1 = new AtomicInteger(0)
        AtomicInteger failedNumber2 = new AtomicInteger(0)
        AtomicInteger failedNumber3 = new AtomicInteger(0)
        threadNumber.times {
            Thread.start {
                try {
                    loopTimes.times {
                        def idResult1 = id1.get('leaf-segment-test').id
                        def idResult2 = id2.get('leaf-segment-test').id
                        def idResult3 = id3.get('leaf-segment-test').id
                        if (idResult1 < 0) {
                            failedNumber1.incrementAndGet()
                        }
                        if (idResult2 < 0) {
                            failedNumber2.incrementAndGet()
                        }
                        if (idResult3 < 0) {
                            failedNumber3.incrementAndGet()
                        }
                        set1 << idResult1
                        set2 << idResult2
                        set3 << idResult3
                    }
                } finally {
                    latch.countDown()
                }
            }
        }
        latch.await()
        def namePre1 = id1.name.padRight(20, ' ')
        def namePre2 = id2.name.padRight(20, ' ')
        def namePre3 = id3.name.padRight(20, ' ')

        log.warn namePre1 + 'first result: ' + set1[0]
        log.warn namePre1 + 'middle result: ' + set1[loopTimes * threadNumber / 2 as int]
        log.warn namePre1 + 'last result: ' + set1[-1]
        log.warn namePre1 + 'failed number: ' + failedNumber1

        log.warn namePre2 + 'first result: ' + set2[0]
        log.warn namePre2 + 'middle result: ' + set2[loopTimes * threadNumber / 2 as int]
        log.warn namePre2 + 'last result: ' + set2[-1]
        log.warn namePre2 + 'failed number: ' + failedNumber2

        log.warn namePre3 + 'first result: ' + set3[0]
        log.warn namePre3 + 'middle result: ' + set3[loopTimes * threadNumber / 2 as int]
        log.warn namePre3 + 'last result: ' + set3[-1]
        log.warn namePre3 + 'failed number: ' + failedNumber3

        expect:
        // get -3 many times
        set1.size() <= loopTimes * threadNumber
        set2.size() <= loopTimes * threadNumber
        set3.size() <= loopTimes * threadNumber
        // first is -3 if there is failed get
        set1.size() + failedNumber1 - (failedNumber1.get() > 0 ? 1 : 0) == loopTimes * threadNumber
        set2.size() + failedNumber2 - (failedNumber2.get() > 0 ? 1 : 0) == loopTimes * threadNumber
        set3.size() + failedNumber3 - (failedNumber3.get() > 0 ? 1 : 0) == loopTimes * threadNumber
        // no exception
        set1.every { it > 0 || it == -3 }
        set2.every { it > 0 || it == -3 }
        set3.every { it > 0 || it == -3 }

        set1.remove(-3L) != null
        set2.remove(-3L) != null
        set3.remove(-3L) != null

        !set1.any { it in set2 }
        !set1.any { it in set3 }

        !set2.any { it in set1 }
        !set2.any { it in set3 }

        !set3.any { it in set1 }
        !set3.any { it in set2 }

        cleanup:
        id1.close()
        id2.close()
        id3.close()
        log.warn namePre1 + ' test done'
        log.warn namePre2 + ' test done'
        log.warn namePre3 + ' test done'
    }
}
