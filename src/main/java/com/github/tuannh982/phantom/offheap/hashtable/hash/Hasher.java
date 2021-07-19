package com.github.tuannh982.phantom.offheap.hashtable.hash;

import java.nio.ByteBuffer;

public interface Hasher {
    long hash(byte[] buffer);
    long hash(ByteBuffer buffer);
    long hash(long address, long offset, int len); // unsafe hash
}
