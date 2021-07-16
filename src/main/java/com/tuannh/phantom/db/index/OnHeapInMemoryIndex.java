package com.tuannh.phantom.db.index;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OnHeapInMemoryIndex implements IndexMap {
    private static class ByteArrayWrapper {
        private final byte[] array;

        private ByteArrayWrapper(byte[] array) {
            this.array = array;
        }

        public static ByteArrayWrapper of(byte[] arr) {
            return new ByteArrayWrapper(arr);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ByteArrayWrapper that = (ByteArrayWrapper) o;
            return Arrays.equals(array, that.array);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(array);
        }
    }

    private final Map<ByteArrayWrapper, IndexMetadata> map;

    public OnHeapInMemoryIndex() {
        this.map = new ConcurrentHashMap<>();
    }

    @Override
    public IndexMetadata get(byte[] key) {
        return map.get(ByteArrayWrapper.of(key));
    }

    @Override
    public void put(byte[] key, IndexMetadata metaData) {
        map.put(ByteArrayWrapper.of(key), metaData);
    }

    @Override
    public IndexMetadata putIfAbsent(byte[] key, IndexMetadata metaData) {
        return map.putIfAbsent(ByteArrayWrapper.of(key), metaData);
    }

    @Override
    public boolean replace(byte[] key, IndexMetadata oldValue, IndexMetadata newValue) {
        if (newValue.getSequenceNumber() > oldValue.getSequenceNumber()) {
            return map.replace(ByteArrayWrapper.of(key), oldValue, newValue);
        } else {
            return false;
        }
    }

    @Override
    public void delete(byte[] key) {
        map.remove(ByteArrayWrapper.of(key));
    }

    @Override
    public void close() throws IOException {
        // NOOP
    }
}
