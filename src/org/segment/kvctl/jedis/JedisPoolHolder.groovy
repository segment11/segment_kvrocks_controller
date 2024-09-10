package org.segment.kvctl.jedis

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.segment.kvctl.App
import org.segment.kvctl.Conf
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig

import java.time.Duration

@CompileStatic
@Singleton
@Slf4j
class JedisPoolHolder {
    // may be thousands, clear unused will be better
    private Map<String, JedisPool> cached = [:]

    synchronized JedisPool create(String host, int port, Integer timeoutMills = null) {
        String key = host + ':' + port
        def client = cached[key]
        if (client) {
            return client
        }

        def conf = new JedisPoolConfig()
        // job need a lot connections
        conf.maxTotal = Conf.instance.getInt('jedis.pool.maxTotal', 10)
        conf.maxIdle = Conf.instance.getInt('jedis.pool.maxIdle', 5)
        conf.maxWait = Duration.ofMillis(Conf.instance.getInt('jedis.pool.maxWait.ms', 5000))

        def timeoutMillsFinal = timeoutMills != null ? timeoutMills :
                Conf.instance.getInt('jedis.read.timeout.ms', 10000)
        def one = new JedisPool(conf, host, port, timeoutMillsFinal, App.instance.password)
        log.info 'connected jedis pool - {}', key
        cached[key] = one
        one
    }

    synchronized void removeUseless(List<String> usedIpList) {
        List<String> needRemoveKeyList = []
        for (entry in cached) {
            def key = entry.key
            def ip = key.split(':')[0]

            if (!(ip in usedIpList)) {
                needRemoveKeyList << key
            }
        }
        for (key in needRemoveKeyList) {
            def v = cached.remove(key)
            try {
                v.close()
            } catch (Exception e) {
                log.error('close jedis pool error, ex: ' + e.message)
            }
        }
    }

    static <R> R useRedisPool(JedisPool jedisPool, JedisCallback<R> callback) {
        Jedis jedis = jedisPool.resource
        try {
            callback.call(jedis)
        } finally {
            jedis.close()
        }
    }

    void close() {
        cached.each { k, v ->
            log.info 'ready to close redis pool - {}', k
            v.close()
            log.info 'done close redis pool - {}', k
        }
    }
}
