package com.github.tuannh982.phantom.offheap.hashtable.segment;

import com.github.tuannh982.phantom.commons.concurrent.RLock;
import com.github.tuannh982.phantom.offheap.hashtable.KeyBuffer;
import com.github.tuannh982.phantom.offheap.hashtable.ValueSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Segment<V> implements Closeable {
    // primary
    private final int maxKeySize;
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

    public Segment(int maxKeySize, int fixedValueSize, int memoryChunkSize, ValueSerializer<V> valueSerializer, int entryTableSizePerSegment) {
        this.maxKeySize = maxKeySize;
        this.fixedValueSize = fixedValueSize;
        this.memoryChunkSize = memoryChunkSize;
        this.valueSerializer = valueSerializer;
        this.entryTableSizePerSegment = entryTableSizePerSegment;
        this.lock = new RLock();
        //
        this.entryTable = EntryTable.allocate(entryTableSizePerSegment);
        this.chunks = new ArrayList<>();
        this.currentChunkIndex = -1;
    }

    public V get(KeyBuffer keyBuffer) throws IOException {
        boolean rlock = lock.lock();
        try {
            Address address = entryTable.getEntry(keyBuffer.hash());
            while (address.getChunkIndex() >= 0) {
                Chunk chunk = chunks.get(address.getChunkIndex());
                if (chunk.compareKey(address.getChunkOffset(), keyBuffer.buffer())) {
                    return valueSerializer.deserialize(chunk.valueByteBuffer(address.getChunkOffset()));
                }
                address = chunk.getNextAddress(address.getChunkOffset());
            }
            return null;
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
        entryTable.close();
        for (Chunk chunk : chunks) {
            chunk.close();
        }
        Collections.fill(chunks, null);
    }
}