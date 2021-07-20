package io.github.tuannh982.phantom.offheap.hashtable.segment;

import io.github.tuannh982.phantom.commons.concurrent.RLock;
import io.github.tuannh982.phantom.offheap.hashtable.KeyBuffer;
import io.github.tuannh982.phantom.offheap.hashtable.ValueSerializer;

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
    // free list
    private final FreeList freeList;

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
        this.freeList = new FreeList();
    }

    public V get(KeyBuffer keyBuffer) throws IOException {
        boolean rlock = lock.lock();
        try {
            Address address = entryTable.getEntry(keyBuffer.hash());
            while (!address.isNullAddress()) {
                Chunk chunk = chunks.get(address.getChunkIndex());
                if (chunk.compareKey(address.getChunkOffset(), keyBuffer.buffer())) {
                    return valueSerializer.deserialize(chunk.readValue(address.getChunkOffset()));
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
            Address previousAddress = null;
            Address address = entryTable.getEntry(keyBuffer.hash());
            while (!address.isNullAddress()) {
                Chunk chunk = chunks.get(address.getChunkIndex());
                Address nextAddress = chunk.getNextAddress(address.getChunkOffset());
                if (chunk.compareKey(address.getChunkOffset(), keyBuffer.buffer())) {
                    // remove address by short circuit the link between previous address and next address
                    if (previousAddress == null) {
                        // current address is entry table head, so just overwrite the entry table
                        entryTable.setEntry(keyBuffer.hash(), nextAddress);
                    } else {
                        chunks.get(previousAddress.getChunkIndex()).setNextAddress(previousAddress.getChunkOffset(), nextAddress);
                        freeList.offer(address); // add address to free list
                    }
                }
                previousAddress = address;
                address = nextAddress;
            }
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

    private class FreeList {
        private Address freeListPtr = Address.NULL_VALUE;
        private int size = 0;

        public void offer(Address address) {
            Chunk chunk = chunks.get(address.getChunkIndex());
            chunk.setNextAddress(address.getChunkOffset(), freeListPtr);
            freeListPtr = address;
            size++;
        }

        public Address poll() {
            if (freeListPtr.isNullAddress()) {
                return freeListPtr;
            } else {
                Address ret = freeListPtr;
                Chunk chunk = chunks.get(freeListPtr.getChunkIndex());
                freeListPtr = chunk.getNextAddress(freeListPtr.getChunkOffset());
                size--;
                return ret;
            }
        }

        public Address peek() {
            return freeListPtr;
        }

        public int size() {
            return size;
        }

        public void clear() {
            freeListPtr = Address.NULL_VALUE;
        }
    }
}
