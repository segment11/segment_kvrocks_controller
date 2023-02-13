package org.segment.kvctl.cli

import groovy.transform.CompileStatic
import groovy.transform.TupleConstructor
import org.apache.commons.cli.CommandLine

@CompileStatic
@TupleConstructor
class BaseCommandTaskRunner implements CommandTaskRunner {
    String name
    CommandLineMatcher matcher
    CommandTaskRunner one

    @Override
    void run(CommandLine cmd) {
        one.run(cmd)
        true
    }
}
