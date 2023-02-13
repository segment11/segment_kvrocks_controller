package org.segment.kvctl.cli

import groovy.transform.CompileStatic
import org.apache.commons.cli.CommandLine

@CompileStatic
@Singleton
class CommandTaskRunnerHolder {
    List<BaseCommandTaskRunner> list = []

    synchronized void add(String name, CommandLineMatcher matcher, CommandTaskRunner one) {
        def old = list.find { it.name == name }
        if (old) {
            list.remove(old)
        }
        list << new BaseCommandTaskRunner(name, matcher, one)
    }

    boolean run(CommandLine cmd) {
        for (one in list) {
            if (one.matcher.isMatch(cmd)) {
                one.run(cmd)
                return true
            }
        }
        false
    }
}
