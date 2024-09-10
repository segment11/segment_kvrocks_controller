package org.segment.leaf

import groovy.transform.CompileStatic

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock

@CompileStatic
class SegmentBuffer {
    private String bizTag

    String getBizTag() {
        return bizTag
    }

    private Segment[] segments //双buffer
    private volatile int currentPos //当前的使用的segment的index
    volatile boolean nextReady //下一个segment是否处于可切换状态
    volatile boolean initOk //是否初始化完成
    private final AtomicBoolean threadRunning //线程是否在运行中
    AtomicBoolean getThreadRunning() {
        return threadRunning
    }
    private final ReadWriteLock lock

    volatile int step
    volatile int minStep
    volatile long updateTimestamp

    SegmentBuffer(String bizTag) {
        this.bizTag = bizTag
        // init
        segments = [new Segment(this), new Segment(this)]
        currentPos = 0
        nextReady = false
        initOk = false
        threadRunning = new AtomicBoolean(false)
        lock = new ReentrantReadWriteLock()
    }

    Segment current() {
        segments[currentPos]
    }

    Segment next() {
        segments[nextPos()]
    }

    int currentPos() {
        currentPos
    }

    int nextPos() {
        (currentPos + 1) % 2
    }

    void switchPos() {
        currentPos = nextPos()
    }

    Lock rLock() {
        lock.readLock()
    }

    Lock wLock() {
        lock.writeLock()
    }

    @Override
    String toString() {
        final StringBuilder sb = new StringBuilder("SegmentBuffer{")
        sb.append("key='").append(bizTag).append('\'')
        sb.append(", segments=").append(Arrays.toString(segments))
        sb.append(", currentPos=").append(currentPos)
        sb.append(", nextReady=").append(nextReady)
        sb.append(", initOk=").append(initOk);
        sb.append(", threadRunning=").append(threadRunning)
        sb.append(", step=").append(step)
        sb.append(", minStep=").append(minStep)
        sb.append(", updateTimestamp=").append(updateTimestamp)
        sb.append('}')
        sb.toString()
    }
}
