package io.github.tuannh982.phantom.offheap.hashtable;

import io.github.tuannh982.phantom.commons.number.NumberUtils;
import io.github.tuannh982.phantom.commons.os.OSConstants;
import io.github.tuannh982.phantom.offheap.hashtable.segment.Segment;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
@Getter
public class OffHeapHashTable<V> implements HashTable<V> {
    private final HashTableOptions<V> options;
    private final int segmentCount;
    private final List<Segment<V>> segments;
    private final int segmentBitCount;
    private final int segmentBitShift;
    private final long segmentBitMask;

    public OffHeapHashTable(HashTableOptions<V> options) {
        this.options = options;
        segmentCount = options.getSegmentCount();
        if (!NumberUtils.isPowerOf2(segmentCount)) {
            throw new IllegalStateException("segment count must be power of 2");
        }
        segments = new ArrayList<>(segmentCount);
        for (int i = 0; i < segmentCount; i++) {
            segments.add(new Segment<>(
                    options.getMaxKeySize(),
                    options.getFixedValueSize(),
                    options.getMemoryChunkSize(),
                    options.getValueSerializer(),
                    options.getEntryTableSizePerSegment()
            ));
        }
        segmentBitCount = NumberUtils.bitCount(segmentCount);
        segmentBitShift = OSConstants.WORD_SIZE_AS_BIT - segmentBitCount;
        segmentBitMask = ((long) segmentCount - 1) << segmentBitShift;
    }

    private Segment<V> getSegment(long hash) {
        int index = (int)((hash & segmentBitMask) >>> segmentBitShift);
        return segments.get(index);
    }

    @Override
    public V get(KeyBuffer keyBuffer) {
        return getSegment(keyBuffer.hash()).get(keyBuffer);
    }

    @Override
    public void put(KeyBuffer keyBuffer, V value) {
        getSegment(keyBuffer.hash()).put(keyBuffer, value);
    }

    @Override
    public V putIfAbsent(KeyBuffer keyBuffer, V value) {
        return getSegment(keyBuffer.hash()).putIfAbsent(keyBuffer, value);
    }

    @Override
    public boolean replace(KeyBuffer keyBuffer, V oldValue, V newValue) {
        return getSegment(keyBuffer.hash()).replace(keyBuffer, oldValue, newValue);
    }

    @Override
    public void remove(KeyBuffer keyBuffer) {
        getSegment(keyBuffer.hash()).delete(keyBuffer);
    }

    @Override
    public void close() throws IOException {
        for (Segment<V> segment : segments) {
            segment.close();
        }
        Collections.fill(segments, null);
    }
}
