package com.tuannh.phantom.db.index;

import com.tuannh.phantom.db.DBException;

import java.io.Closeable;

public interface IndexMap extends Closeable {
    IndexMetadata get(byte[] key) throws DBException;
    void put(byte[] key, IndexMetadata metaData) throws DBException;
    IndexMetadata putIfAbsent(byte[] key, IndexMetadata metaData) throws DBException;
    boolean replace(byte[] key, IndexMetadata oldValue, IndexMetadata newValue) throws DBException;
    void delete(byte[] key) throws DBException;
}
