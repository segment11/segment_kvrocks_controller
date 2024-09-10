package org.segment.kvctl.db

import groovy.transform.CompileStatic
import org.segment.d.D
import org.segment.d.Ds
import org.segment.d.dialect.MySQLDialect
import org.segment.d.Record

@CompileStatic
class BaseRecord<V extends BaseRecord> extends Record<V> {
    @Override
    String pk() {
        'id'
    }

    @Override
    D useD() {
        if (this.d) {
            return this.d
        }
        new D(Ds.one('default_ds'), new MySQLDialect())
    }
}
