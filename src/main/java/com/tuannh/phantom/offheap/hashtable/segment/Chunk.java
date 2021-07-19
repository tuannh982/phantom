package com.tuannh.phantom.offheap.hashtable.segment;

import com.tuannh.phantom.commons.unsafe.UnsafeWrapper;

import java.io.Closeable;
import java.io.IOException;

public class Chunk implements Closeable {
    // primary
    private final int fixedKeySize;
    private final int fixedValueSize;
    private final int memoryChunkSize;
    private final long address;
    private final int writeOffset;

    private Chunk(int fixedKeySize, int fixedValueSize, int memoryChunkSize, long address) {
        this.fixedKeySize = fixedKeySize;
        this.fixedValueSize = fixedValueSize;
        this.memoryChunkSize = memoryChunkSize;
        this.address = address;
        this.writeOffset = 0;
    }

    public static Chunk allocate(int fixedKeySize, int fixedValueSize, int memoryChunkSize) {
        long address = UnsafeWrapper.malloc(memoryChunkSize);
        return new Chunk(fixedKeySize, fixedValueSize, memoryChunkSize, address);
    }

    @Override
    public void close() throws IOException {
        UnsafeWrapper.free(address);
    }
}
