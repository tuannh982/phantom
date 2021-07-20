package io.github.tuannh982.phantom.offheap.hashtable;

import java.io.Closeable;
import java.io.IOException;

public interface HashTable<V> extends Closeable {
    V get(KeyBuffer keyBuffer) throws IOException;
    void put(KeyBuffer keyBuffer, V value) throws IOException;
    V putIfAbsent(KeyBuffer keyBuffer, V value) throws IOException;
    boolean replace(KeyBuffer keyBuffer, V oldValue, V newValue) throws IOException;
    void remove(KeyBuffer keyBuffer) throws IOException;
}
