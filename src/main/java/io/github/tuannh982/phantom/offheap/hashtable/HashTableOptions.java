package io.github.tuannh982.phantom.offheap.hashtable;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class HashTableOptions<V> {
    private final int memoryChunkSize;
    private final int maxKeySize;
    private final int fixedValueSize;
    private final int entryTableSizePerSegment;
    private final ValueSerializer<V> valueSerializer;
}
