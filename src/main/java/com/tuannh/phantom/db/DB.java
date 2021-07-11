package com.tuannh.phantom.db;

import java.io.Closeable;

public interface DB extends Closeable {
    byte[] get(byte[] key) throws DBException;
    boolean put(byte[] key, byte[] value) throws DBException;
    boolean delete(byte[] key) throws DBException;
}
