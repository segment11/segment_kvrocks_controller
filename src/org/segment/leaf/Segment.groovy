package org.segment.leaf

import groovy.transform.CompileStatic

import java.util.concurrent.atomic.AtomicLong

@CompileStatic
class Segment {
    // range (max - step)..max
    AtomicLong value = new AtomicLong(0)
    volatile long max
    volatile int step

    void setRange(long max, int step) {
        this.max = max
        this.step = step
        this.value.set(max - step)
    }

    private SegmentBuffer buffer

    SegmentBuffer getBuffer() {
        return buffer
    }

    Segment(SegmentBuffer buffer) {
        this.buffer = buffer
    }

    long getIdle() {
        this.max - value.get()
    }

    @Override
    String toString() {
        "Segment:value=${value.get()},max=${max},step=${step}"
    }
}
