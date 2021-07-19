package com.tuannh.phantom.offheap.hashtable.segment;

import com.tuannh.phantom.commons.concurrent.RLock;
import com.tuannh.phantom.offheap.hashtable.KeyBuffer;
import com.tuannh.phantom.offheap.hashtable.ValueSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Segment<V> implements Closeable {
    // primary
    private final int fixedKeySize;
    private final int fixedValueSize;
    private final int memoryChunkSize;
    private final ValueSerializer<V> valueSerializer;
    private final int entryTableSizePerSegment;
    // lock
    private final RLock lock;
    //
    private EntryTable entryTable;
    private final List<Chunk> chunks;
    private int currentChunkIndex;

    public Segment(int fixedKeySize, int fixedValueSize, int memoryChunkSize, ValueSerializer<V> valueSerializer, int entryTableSizePerSegment) {
        this.fixedKeySize = fixedKeySize;
        this.fixedValueSize = fixedValueSize;
        this.memoryChunkSize = memoryChunkSize;
        this.valueSerializer = valueSerializer;
        this.entryTableSizePerSegment = entryTableSizePerSegment;
        this.lock = new RLock();
        //
        this.chunks = new ArrayList<>();
    }

    public V get(KeyBuffer keyBuffer) throws IOException {
        boolean rlock = lock.lock();
        try {
            // TODO
        } finally {
            lock.release(rlock);
        }
    }

    public void put(KeyBuffer keyBuffer, V value) throws IOException {
        boolean rlock = lock.lock();
        try {
            // TODO
        } finally {
            lock.release(rlock);
        }
    }

    public V putIfAbsent(KeyBuffer keyBuffer, V oldValue, V newValue) throws IOException {
        boolean rlock = lock.lock();
        try {
            // TODO
        } finally {
            lock.release(rlock);
        }
    }

    public boolean replace(KeyBuffer keyBuffer, V value) throws IOException {
        boolean rlock = lock.lock();
        try {
            // TODO
        } finally {
            lock.release(rlock);
        }
    }

    public void delete(KeyBuffer keyBuffer) throws IOException {
        boolean rlock = lock.lock();
        try {
            // TODO
        } finally {
            lock.release(rlock);
        }
    }

    @Override
    public void close() throws IOException {
        // TODO
    }
}
