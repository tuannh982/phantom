package com.tuannh.phantom.offheap.hashtable;

import com.tuannh.phantom.commons.number.NumberUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OffHeapHashTable<V> implements HashTable<V> {
    private final HashTableOptions<V> options;
    private final int segmentCount;
    private final List<Segment<V>> segments;

    public OffHeapHashTable(HashTableOptions<V> options) {
        this.options = options;
        segmentCount = NumberUtils.roundUpToPowerOf2(2 * Runtime.getRuntime().availableProcessors());
        segments = new ArrayList<>(segmentCount);
        for (int i = 0; i < segmentCount; i++) {
            // allocate segment
            // segment[i] = new Segment
        }
    }

    @Override
    public boolean put(KeyBuffer keyBuffer, V value) {
        return false;
    }

    @Override
    public boolean putIfAbsent(KeyBuffer keyBuffer, V value) {
        return false;
    }

    @Override
    public boolean replace(KeyBuffer keyBuffer, V value) {
        return false;
    }

    @Override
    public boolean delete(KeyBuffer keyBuffer) {
        return false;
    }

    @Override
    public V get(KeyBuffer keyBuffer) {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
