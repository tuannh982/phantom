package com.tuannh.phantom.offheap.hashtable;

import java.io.Closeable;

public interface HashTable<V> extends Closeable {
    boolean put(KeyBuffer keyBuffer, V value);
    boolean putIfAbsent(KeyBuffer keyBuffer, V value);
    boolean replace(KeyBuffer keyBuffer, V value);
    boolean delete(KeyBuffer keyBuffer);
    V get(KeyBuffer keyBuffer);
}
