package com.tuannh.phantom.db;

import java.io.Closeable;
import java.io.IOException;

public interface DB extends Closeable {
    byte[] get(byte[] key) throws DBException, IOException;
    boolean putIfAbsent(byte[] key, byte[] value) throws DBException, IOException;
    boolean replace(byte[] key, byte[] value) throws DBException, IOException;
    void delete(byte[] key) throws DBException, IOException;
}
