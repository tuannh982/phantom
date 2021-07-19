package com.github.tuannh982.phantom.offheap.hashtable;

import com.github.tuannh982.phantom.offheap.hashtable.hash.Hasher;

// raw value buffer
public class KeyBuffer {
    private final byte[] buffer;
    private final long hash;

    public KeyBuffer(byte[] buffer, Hasher hasher) {
        this.buffer = buffer;
        if (buffer.length > Byte.MAX_VALUE) {
            throw new IllegalStateException("key size must not greater than " + Byte.MAX_VALUE);
        }
        this.hash = hasher.hash(buffer);
    }

    public byte[] buffer() {
        return buffer;
    }

    public long hash() {
        return hash;
    }
}
