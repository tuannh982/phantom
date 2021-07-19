package com.github.tuannh982.phantom.db.internal;

import com.github.tuannh982.phantom.offheap.hashtable.hash.Hasher;
import com.github.tuannh982.phantom.offheap.hashtable.hash.Murmur3;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class PhantomDBOptions {
    private final int fixedKeySize;
    private final int dataFlushThreshold;
    private final int maxFileSize;
    private final int maxTombstoneFileSize;
    private final int numberOfIndexingThread;
    private final float compactionThreshold;
    private final Hasher hasher = new Murmur3();
}