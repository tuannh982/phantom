package com.tuannh.phantom.db.index;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OnHeapInMemoryIndex implements IndexMap {
    private final Map<byte[], IndexMetadata> map;

    public OnHeapInMemoryIndex() {
        this.map = new ConcurrentHashMap<>();
    }

    @Override
    public IndexMetadata get(byte[] key) {
        return map.get(key);
    }

    @Override
    public void put(byte[] key, IndexMetadata metaData) {
        map.put(key, metaData);
    }

    @Override
    public IndexMetadata putIfAbsent(byte[] key, IndexMetadata metaData) {
        return map.putIfAbsent(key, metaData);
    }

    @Override
    public boolean replace(byte[] key, IndexMetadata oldValue, IndexMetadata newValue) {
        if (newValue.getSequenceNumber() > oldValue.getSequenceNumber()) {
            return map.replace(key, oldValue, newValue);
        } else {
            return false;
        }
    }

    @Override
    public void delete(byte[] key) {
        map.remove(key);
    }

    @Override
    public void close() throws IOException {
        // NOOP
    }
}
