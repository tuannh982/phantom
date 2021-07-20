package io.github.tuannh982.phantom.db.index;

import java.io.Closeable;
import java.io.IOException;

public interface IndexMap extends Closeable {
    IndexMetadata get(byte[] key);
    void put(byte[] key, IndexMetadata metadata) throws IOException;
    IndexMetadata putIfAbsent(byte[] key, IndexMetadata metadata) throws IOException;
    boolean replace(byte[] key, IndexMetadata oldValue, IndexMetadata newValue) throws IOException;
    void delete(byte[] key) throws IOException;
}
