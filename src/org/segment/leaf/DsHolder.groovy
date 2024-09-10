package org.segment.leaf

import groovy.transform.CompileStatic

import javax.sql.DataSource

@CompileStatic
@Singleton
class DsHolder {
    DataSource dataSource
}
