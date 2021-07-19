package io.github.tuannh982.phantom.db.index;

import java.io.Closeable;

public interface IndexMap extends Closeable {
    IndexMetadata get(byte[] key);
    void put(byte[] key, IndexMetadata metaData);
    IndexMetadata putIfAbsent(byte[] key, IndexMetadata metaData);
    boolean replace(byte[] key, IndexMetadata oldValue, IndexMetadata newValue);
    void delete(byte[] key);
}
