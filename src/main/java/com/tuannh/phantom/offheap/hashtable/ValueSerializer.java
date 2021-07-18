package com.tuannh.phantom.offheap.hashtable;

import java.nio.ByteBuffer;

public interface ValueSerializer<V> {
    ByteBuffer serialize(V value);
    V deserialize(ByteBuffer buffer);
    int serializedSize();
}
