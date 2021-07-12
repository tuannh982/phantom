package com.tuannh.phantom.db.internal;

import com.tuannh.phantom.offheap.hashtable.hash.Hasher;
import com.tuannh.phantom.offheap.hashtable.hash.Murmur3;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class PhantomDBOptions {
    private int fixedKeySize;
    private int dataFlushThreshold;
    private final Hasher hasher = new Murmur3();
}
