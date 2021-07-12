package com.tuannh.phantom.db;

import com.tuannh.phantom.offheap.hashtable.KeyBuffer;

import java.io.Closeable;

public interface DB extends Closeable {
    byte[] get(byte[] key) throws DBException;
    boolean putIfAbsent(byte[] key, byte[] value) throws DBException;
    boolean replace(byte[] key, byte[] value) throws DBException;
    boolean delete(byte[] key) throws DBException;
}
