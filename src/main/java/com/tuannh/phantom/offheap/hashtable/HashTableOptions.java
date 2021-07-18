package com.tuannh.phantom.offheap.hashtable;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class HashTableOptions<V> {
    private int memoryChunkSize;
    private int fixedKeySize;
    private int fixedValueSize;
    private ValueSerializer<V> valueSerializer;
}
