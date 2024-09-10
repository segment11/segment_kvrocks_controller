package org.segment.leaf

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.segment.kvctl.Conf

import javax.sql.DataSource
import java.util.concurrent.*

@CompileStatic
@Slf4j
class IDGenerator {

    String name

    DataSource dataSource

    volatile boolean initOK = false

    private ExecutorService service = new ThreadPoolExecutor(5, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
            new SynchronousQueue<Runnable>(), new UpdateThreadFactory())

    static class UpdateThreadFactory implements ThreadFactory {
        private static int threadInitNumber = 0

        private static synchronized int nextThreadNum() {
            threadInitNumber++
        }

        @Override
        Thread newThread(Runnable r) {
            new Thread(r, "Thread-Segment-Update-" + nextThreadNum())
        }
    }

    private Map<String, SegmentBuffer> cache = new ConcurrentHashMap<String, SegmentBuffer>()

    // for monitor
    Map<String, SegmentBuffer> getCache() {
        return cache
    }

    // for monitor
    List<OneBiz> getAllBizList() {
        OneBiz.getAllList()
    }

    private final static Result RESULT_NOT_INIT_YET = new Result(-1, Result.Status.EXCEPTION)
    private final static Result RESULT_BIZ_TAG_NOT_EXIST = new Result(-2, Result.Status.EXCEPTION)
    private final static Result RESULT_CACHE_NOT_READY = new Result(-3, Result.Status.EXCEPTION)

    ScheduledExecutorService intervalService

    void init() {
        log.info 'id generator init ing...'
        assert dataSource
        DsHolder.instance.dataSource = dataSource

        updateCacheFromDb()
        initOK = true
        updateCacheFromDbInterval()
    }

    void close() {
        if (intervalService) {
            intervalService.shutdown()
            log.info 'stop interval load db tags to cache'
        }
        service.shutdown()
        log.info 'stop load next segment if current idle is less'
    }

    Result get(String bizTag) {
        if (!initOK) {
            return RESULT_NOT_INIT_YET
        }
        if (!cache.containsKey(bizTag)) {
            return RESULT_BIZ_TAG_NOT_EXIST
        }

        def buffer = cache.get(bizTag)
        if (!buffer.isInitOk()) {
            synchronized (buffer) {
                if (!buffer.isInitOk()) {
                    try {
                        def segment = buffer.current()
                        updateSegmentFromDb(bizTag, segment)
                        buffer.setInitOk(true)
                        log.info 'done load tag {} from db to segment {}', bizTag, segment
                    } catch (Exception e) {
                        log.error 'fail update segment from db - ' + bizTag, e
                    }
                }
            }
        }
        def result = getIdFromSegmentBuffer(buffer)
        // try again if next segment load not ready, because it's async
        if (result.status == RESULT_CACHE_NOT_READY.status) {
            def ms = Conf.instance.getInt('wait_ms_when_next_segment_not_ready', 10)
            log.warn 'wait a while as next segment not ready {}', ms
            Thread.sleep(ms)
            result = getIdFromSegmentBuffer(buffer)
        }
        result
    }

    private Result getIdFromSegmentBuffer(SegmentBuffer buffer) {
        while (true) {
            buffer.rLock().lock()
            try {
                def segment = buffer.current()

                int segmentCurrentIdleLeftPercent = Conf.instance.getInt('segment_current_idle_left_percent', 90)
                // need load next segment
                if (!buffer.isNextReady() &&
                        (segment.idle < (segmentCurrentIdleLeftPercent / 100 * segment.step)) &&
                        buffer.threadRunning.compareAndSet(false, true)
                ) {
                    service.submit {
                        def next = buffer.next()
                        boolean isUpdateOk = false
                        try {
                            updateSegmentFromDb(buffer.bizTag, next)
                            isUpdateOk = true
                        } catch (Exception e) {
                            log.error 'update segment from db error - ' + buffer.bizTag, e
                        } finally {
                            if (isUpdateOk) {
                                buffer.wLock().lock()
                                buffer.nextReady = true
                                buffer.threadRunning.set(false)
                                buffer.wLock().unlock()
                            } else {
                                buffer.threadRunning.set(false)
                            }
                        }
                    }
                }
                long value = segment.value.getAndIncrement()
                if (value < segment.getMax()) {
                    return new Result(value, Result.Status.SUCCESS)
                }
            } finally {
                buffer.rLock().unlock()
            }

            waitAndSleep(buffer)
            buffer.wLock().lock()
            try {
                final def segment = buffer.current()
                long value = segment.value.getAndIncrement()
                if (value < segment.max) {
                    return new Result(value, Result.Status.SUCCESS)
                }
                if (buffer.nextReady) {
                    buffer.switchPos()
                    buffer.nextReady = false
                } else {
                    log.warn 'segments both not ready {}', buffer
                    return RESULT_CACHE_NOT_READY
                }
            } finally {
                buffer.wLock().unlock();
            }
        }
    }

    private void waitAndSleep(SegmentBuffer buffer) {
        int roll = 0;
        while (buffer.threadRunning.get()) {
            roll += 1;
            if (roll > 10000) {
                try {
                    TimeUnit.MILLISECONDS.sleep(10)
                    break
                } catch (InterruptedException e) {
                    log.error 'thread {} interrupted', Thread.currentThread().name
                    break
                }
            }
        }
    }

    private void updateSegmentFromDb(String bizTag, Segment segment) {
        def beginMs = System.currentTimeMillis()
        def buffer = segment.buffer
        OneBiz one
        if (!buffer.initOk) {
            one = OneBiz.updateMaxId(bizTag)
            buffer.step = one.step
            buffer.minStep = one.step
        } else if (buffer.updateTimestamp == 0) {
            one = OneBiz.updateMaxId(bizTag)
            buffer.step = one.step
            buffer.minStep = one.step
            buffer.updateTimestamp = System.currentTimeMillis()
        } else {
            long segmentDuration = Conf.instance.getInt('segment_duration', 10 * 60 * 1000)
            long segmentMaxStep = Conf.instance.getInt('segment_max_step', 10 * 10000)

            long duration = System.currentTimeMillis() - buffer.updateTimestamp
            int newStep = buffer.step
            // if get from db too much, need expand step
            if (duration < segmentDuration) {
                if (newStep * 2 > segmentMaxStep) {
                    //do nothing
                } else {
                    // expand two times as old
                    newStep = newStep * 2
                }
            } else if (duration < segmentDuration * 2) {
                //do nothing with newStep
            } else {
                if (newStep / 2 >= buffer.minStep) {
                    newStep = (newStep / 2) as int
                }
            }
            log.info('expand tag {} step from {} to {} as get from db duration too small {}min', bizTag,
                    buffer.step, newStep, String.format("%.2f", ((double) duration / (1000 * 60))))

            one = OneBiz.updateMaxIdByCustomStep(bizTag, newStep)
            buffer.step = newStep
            buffer.minStep = one.step
            buffer.updateTimestamp = System.currentTimeMillis()
        }

        segment.setRange(one.maxId, buffer.step)
        log.info ' load db tag {} to segment {} cost ms {}', bizTag, segment.toString(), (System.currentTimeMillis() - beginMs)
    }

    // load tag when db records changed
    private void updateCacheFromDbInterval() {
        intervalService = Executors.newSingleThreadScheduledExecutor { r ->
            def t = new Thread(r)
            t.name = 'check-idCache-thread'
            t.daemon = true
            t
        }
        intervalService.scheduleWithFixedDelay({
            updateCacheFromDb()
        }, 60, 60, TimeUnit.SECONDS)
    }

    synchronized void updateCacheFromDb() {
        log.info 'update cache from db'

        def bizTags = OneBiz.getAllBizTagList()
        if (!bizTags) {
            return
        }

        def cachedTags = new ArrayList(cache.keySet())

        // load from db if not load to cache yet
        def needLoadTags = bizTags.findAll {
            !cachedTags.contains(it)
        }
        for (bizTag in needLoadTags) {
            def buffer = new SegmentBuffer(bizTag)
            buffer.current().setRange(0, 0)
            cache.put(bizTag, buffer)
            log.info 'add biz tag {} from db to cache {}', bizTag, buffer
        }

        // remove cache if not config in db
        for (cachedTag in cachedTags) {
            if (!bizTags.contains(cachedTag)) {
                cache.remove(cachedTag)
                log.info 'remove cache tag not in db {}', cachedTag
            }
        }
    }
}
