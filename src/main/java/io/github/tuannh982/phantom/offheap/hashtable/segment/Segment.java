package io.github.tuannh982.phantom.offheap.hashtable.segment;

import io.github.tuannh982.phantom.commons.concurrent.RLock;
import io.github.tuannh982.phantom.offheap.hashtable.KeyBuffer;
import io.github.tuannh982.phantom.offheap.hashtable.ValueSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@SuppressWarnings("java:S1068")
public class Segment<V> implements Closeable {
    // primary
    private final int maxKeySize;
    private final int fixedValueSize;
    private final int memoryChunkSize;
    private final ValueSerializer<V> valueSerializer;
    private final int entryTableSizePerSegment;
    private final int entrySize;
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
        this.entrySize = Address.SERIALIZED_SIZE + 1 + maxKeySize + fixedValueSize; // address(6), key_size(1), max_key_size, fixed_value_size
        this.lock = new RLock();
        //
        this.entryTable = EntryTable.allocate(entryTableSizePerSegment);
        this.chunks = new ArrayList<>();
        this.currentChunkIndex = -1;
        this.freeList = new FreeList();
    }

    public V get(KeyBuffer keyBuffer) {
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

    public void put(KeyBuffer keyBuffer, V value) {
        byte[] serializedValue = valueSerializer.serialize(value).array();
        boolean rlock = lock.lock();
        try {
            Address address = entryTable.getEntry(keyBuffer.hash());
            Address entryTableEntry = address;
            while (!address.isNullAddress()) {
                Chunk chunk = chunks.get(address.getChunkIndex());
                if (chunk.compareKey(address.getChunkOffset(), keyBuffer.buffer())) {
                    chunk.setValue(address.getChunkOffset(), serializedValue);
                    return;
                }
                address = chunk.getNextAddress(address.getChunkOffset());
            }
            write(keyBuffer.hash(), keyBuffer.buffer(), serializedValue, entryTableEntry);
        } finally {
            lock.release(rlock);
        }
    }

    public V putIfAbsent(KeyBuffer keyBuffer, V value) {
        byte[] serializedValue = valueSerializer.serialize(value).array();
        boolean rlock = lock.lock();
        try {
            Address address = entryTable.getEntry(keyBuffer.hash());
            Address entryTableEntry = address;
            while (!address.isNullAddress()) {
                Chunk chunk = chunks.get(address.getChunkIndex());
                if (chunk.compareKey(address.getChunkOffset(), keyBuffer.buffer())) {
                    return valueSerializer.deserialize(chunk.readValue(address.getChunkOffset()));
                }
                address = chunk.getNextAddress(address.getChunkOffset());
            }
            write(keyBuffer.hash(), keyBuffer.buffer(), serializedValue, entryTableEntry);
            return null;
        } finally {
            lock.release(rlock);
        }
    }

    private void write(long hash, byte[] key, byte[] value, Address entryTableEntry) {
        Address newHeadAddress = null;
        if (freeList.size() > 0) {
            Address freeAddress = freeList.poll();
            chunks.get(freeAddress.getChunkIndex()).writeEntry(key, value, entryTableEntry);
            newHeadAddress = freeAddress;
        } else {
            if (currentChunkIndex == -1 || chunks.get(currentChunkIndex).remaining() < entrySize) {
                // rollover new chunk
                if (chunks.size() > Short.MAX_VALUE) {
                    throw new OutOfMemoryError();
                }
                chunks.add(Chunk.allocate(maxKeySize, fixedValueSize, memoryChunkSize));
                currentChunkIndex++;
            }
            Chunk currentChunk = chunks.get(currentChunkIndex);
            Address newAddress = new Address((short) currentChunkIndex, currentChunk.getWriteOffset());
            currentChunk.writeEntry(key, value, entryTableEntry);
            newHeadAddress = newAddress;
        }
        entryTable.setEntry(hash, newHeadAddress);
    }

    public boolean replace(KeyBuffer keyBuffer, V oldValue, V newValue) {
        byte[] serializedOldValue = valueSerializer.serialize(oldValue).array();
        byte[] serializedNewValue = valueSerializer.serialize(newValue).array();
        boolean rlock = lock.lock();
        try {
            Address address = entryTable.getEntry(keyBuffer.hash());
            while (!address.isNullAddress()) {
                Chunk chunk = chunks.get(address.getChunkIndex());
                if (chunk.compareKey(address.getChunkOffset(), keyBuffer.buffer())) { // found key
                    if (chunk.compareValue(address.getChunkOffset(), serializedOldValue)) { // value match provided value
                        chunk.setValue(address.getChunkOffset(), serializedNewValue);
                        return true;
                    } else { // not match
                        return false;
                    }
                }
                address = chunk.getNextAddress(address.getChunkOffset());
            }
            return false;
        } finally {
            lock.release(rlock);
        }
    }

    public void delete(KeyBuffer keyBuffer) {
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
