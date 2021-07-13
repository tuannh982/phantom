package com.tuannh.phantom.commons.concurrent;

import lombok.NoArgsConstructor;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

// simple version of re-entrance lock
@NoArgsConstructor
public class RLock {
    private static final AtomicLongFieldUpdater<RLock> UPDATER = AtomicLongFieldUpdater.newUpdater(RLock.class, "holder");

    private volatile long holder = 0;

    public boolean lock() {
        long threadId = Thread.currentThread().getId();
        if (threadId == UPDATER.get(this)) {
            return false; // already locked on current thread
        }
        while (true) {
            if (UPDATER.compareAndSet(this, 0, threadId)) {
                return true;
            }
            Thread.yield();
        }
    }

    public void release(boolean lockResult) { // result of latest lock() command
        if (lockResult) {
            long threadId = Thread.currentThread().getId();
            if (!UPDATER.compareAndSet(this, threadId, 0)) {
                throw new AssertionError("could not execute compareAndSet");
            }
        }
    }
}
