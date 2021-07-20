package io.github.tuannh982.phantom.offheap.hashtable;

import java.io.Closeable;
import java.io.IOException;

public interface HashTable<V> extends Closeable {
    V get(KeyBuffer keyBuffer);
    void put(KeyBuffer keyBuffer, V value);
    V putIfAbsent(KeyBuffer keyBuffer, V value);
    boolean replace(KeyBuffer keyBuffer, V oldValue, V newValue);
    void remove(KeyBuffer keyBuffer);
}
