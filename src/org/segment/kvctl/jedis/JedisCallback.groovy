package org.segment.kvctl.jedis

import groovy.transform.CompileStatic
import redis.clients.jedis.Jedis

@CompileStatic
interface JedisCallback {
    Object call(Jedis jedis)
}