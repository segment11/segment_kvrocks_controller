package org.segment.kvctl

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.segment.kvctl.db.JobLogDTO
import org.segment.leaf.IDGenerator

@CompileStatic
@Singleton
@Slf4j
class ClusterVersionHelper {
    private IDGenerator idGenerator

    void init() {
        idGenerator = new IDGenerator()
        idGenerator.dataSource = new JobLogDTO().useD().db.dataSource
        idGenerator.init()
    }

    void load() {
        idGenerator.updateCacheFromDb()
    }

    long get(Integer appId) {
        def result = idGenerator.get(appId.toString())
        def id = result.id
        if (id > 0) {
            return id
        }
        // only try once
        def result2 = idGenerator.get(appId.toString())
        result2.id
    }

    void getUntil(Integer appId, long toVal) {
        int i = 0
        while (get(appId) < toVal) {
            i++
        }
        log.debug 'get until val: {}, loop times: {}, app id: {}', toVal, i, appId
    }

    Set<String> keys() {
        idGenerator.cache.keySet()
    }

    void close() {
        if (idGenerator) {
            idGenerator.close()
        }
    }
}
