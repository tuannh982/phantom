package com.tuannh.phantom.db.index;

import com.tuannh.phantom.db.DBException;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OnHeapInMemoryIndex extends InMemoryIndex {
    private final Map<byte[], IndexMetadata> map;

    public OnHeapInMemoryIndex() {
        this.map = new ConcurrentHashMap<>();
    }

    @Override
    public IndexMetadata get(byte[] key) throws DBException {
        return map.get(key);
    }

    @Override
    public void put(byte[] key, IndexMetadata metaData) throws DBException {
        map.put(key, metaData);
    }

    @Override
    public void delete(byte[] key) throws DBException {
        map.remove(key);
    }

    @Override
    public void close() throws IOException {
        // NOOP
    }
}
