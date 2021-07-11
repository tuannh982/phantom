package com.tuannh.phantom.offheap.hashtable;

import lombok.AllArgsConstructor;

// raw value buffer
@AllArgsConstructor
public class KeyBuffer {
    private final byte[] buffer;
    private final long hash;

    public byte[] buffer() {
        return buffer;
    }

    public long hash() {
        return hash;
    }
}
