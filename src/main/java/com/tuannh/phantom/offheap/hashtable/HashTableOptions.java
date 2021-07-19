package com.tuannh.phantom.offheap.hashtable;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class HashTableOptions<V> {
    private final int memoryChunkSize;
    private final int fixedKeySize;
    private final int fixedValueSize;
    private final int entryTableSizePerSegment;
    private final ValueSerializer<V> valueSerializer;
}
