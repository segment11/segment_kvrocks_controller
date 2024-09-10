package org.segment.leaf

import groovy.sql.Sql
import groovy.transform.CompileStatic

@CompileStatic
class OneBiz {

    String bizTag
    Long maxId
    Integer step
    Date updateTime

    static List<OneBiz> getAllList() {
        def sql = new Sql(DsHolder.instance.dataSource)
        List<OneBiz> r = []
        sql.query('select * from leaf_alloc') { rs ->
            while (rs.next()) {
                def one = new OneBiz()
                one.bizTag = rs.getString('biz_tag')
                one.maxId = rs.getLong('max_id')
                one.step = rs.getInt('step')
                one.updateTime = rs.getDate('update_time')
                r << one
            }
        }
        r
    }

    static OneBiz updateMaxId(String bizTag) {
        def sql = new Sql(DsHolder.instance.dataSource)
        OneBiz one
        sql.withTransaction {
            Object[] args = [bizTag]
            sql.executeUpdate('UPDATE leaf_alloc SET max_id = max_id + step WHERE biz_tag = ?', args)
            def row = sql.firstRow('select * from leaf_alloc where biz_tag = ? limit 1', [bizTag as Object])
            one = new OneBiz()
            one.bizTag = row.get('biz_tag')
            one.maxId = row.get('max_id') as long
            one.step = row.get('step') as int
            one.updateTime = row.get('update_time') as Date
        }
        one
    }

    static OneBiz updateMaxIdByCustomStep(String bizTag, Integer step) {
        def sql = new Sql(DsHolder.instance.dataSource)
        OneBiz one
        sql.withTransaction {
            Object[] args = [step, bizTag]
            sql.executeUpdate('UPDATE leaf_alloc SET max_id = max_id + ? WHERE biz_tag = ?', args)
            def row = sql.firstRow('select * from leaf_alloc where biz_tag = ? limit 1', [bizTag as Object])
            one = new OneBiz()
            one.bizTag = row.get('biz_tag')
            one.maxId = row.get('max_id') as long
            one.step = row.get('step') as int
            one.updateTime = row.get('update_time') as Date
        }
        one
    }

    static List<String> getAllBizTagList() {
        def sql = new Sql(DsHolder.instance.dataSource)
        List<String> r = []
        sql.query('select biz_tag from leaf_alloc') { rs ->
            while (rs.next()) {
                r << rs.getString('biz_tag')
            }
        }
        r
    }
}
