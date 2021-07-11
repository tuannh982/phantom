package com.tuannh.phantom.offheap.hashtable;

import java.nio.ByteBuffer;

public interface ValueSerializer<V> {
    void serialize(V value, ByteBuffer buffer);
    V deserialize(ByteBuffer buffer);
    int serializedSize();
}
