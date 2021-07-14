package com.tuannh.phantom.db.index;

import com.tuannh.phantom.db.DBException;

import java.io.Closeable;

public abstract class IndexMap implements Closeable {
    public abstract IndexMetadata get(byte[] key) throws DBException;
    public abstract void put(byte[] key, IndexMetadata metaData) throws DBException;
    public abstract IndexMetadata putIfAbsent(byte[] key, IndexMetadata metaData) throws DBException;
    public abstract boolean replace(byte[] key, IndexMetadata oldValue, IndexMetadata newValue) throws DBException;
    public abstract void delete(byte[] key) throws DBException;
}
