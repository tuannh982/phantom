package com.github.tuannh982.phantom.offheap.hashtable.segment;

import com.github.tuannh982.phantom.commons.number.NumberUtils;
import com.github.tuannh982.phantom.commons.unsafe.UnsafeWrapper;
import lombok.Getter;

import java.io.Closeable;
import java.io.IOException;

@Getter
public class EntryTable implements Closeable {
    private final int hashTableSize;
    private final long address;
    private final int mask;

    public EntryTable(int hashTableSize, long address, int mask) {
        this.hashTableSize = hashTableSize;
        this.address = address;
        this.mask = mask;
    }

    public static EntryTable allocate(int hashTableSize) {
        if (!NumberUtils.isPowerOf2(hashTableSize)) {
            throw new IllegalStateException("hash table size must be power of 2");
        }
        int size = hashTableSize * Address.SERIALIZED_SIZE;
        long address = UnsafeWrapper.malloc(size);
        int mask = hashTableSize - 1;
        return new EntryTable(hashTableSize, address, mask);
    }

    public int entryIndex(long hash) {
        return (int) (hash & mask);
    }

    private long entryOffset(int index) {
        return (long) index * Address.SERIALIZED_SIZE;
    }

    public Address getEntry(long hash) {
        long entryAddress = address + entryOffset(entryIndex(hash));
        return Address.deserialize(entryAddress);
    }

    public void setEntry(long hash, Address entry) {
        long entryAddress = address + entryOffset(entryIndex(hash));
        entry.serialize(entryAddress);
    }

    @Override
    public void close() throws IOException {
        UnsafeWrapper.free(address);
    }
}
