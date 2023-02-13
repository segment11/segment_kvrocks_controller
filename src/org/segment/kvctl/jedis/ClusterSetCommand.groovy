package org.segment.kvctl.jedis

import groovy.transform.CompileStatic
import redis.clients.jedis.commands.ProtocolCommand

@CompileStatic
class ClusterSetCommand implements ProtocolCommand {
    private String command

    ClusterSetCommand(String command) {
        this.command = command
    }

    @Override
    byte[] getRaw() {
        command.bytes
    }
}
