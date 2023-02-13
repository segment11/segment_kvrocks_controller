package org.segment.kvctl.cli

import groovy.transform.CompileStatic
import org.apache.commons.cli.CommandLine

@CompileStatic
interface CommandLineMatcher {
    boolean isMatch(CommandLine cmd)
}
