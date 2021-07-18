package com.tuannh.phantom.db;

import java.io.Closeable;
import java.io.IOException;

public interface DB extends Closeable {
    byte[] get(byte[] key) throws IOException;
    boolean put(byte[] key, byte[] value) throws IOException;
    boolean putIfAbsent(byte[] key, byte[] value) throws IOException;
    boolean replace(byte[] key, byte[] value) throws IOException;
    void delete(byte[] key) throws IOException;
}
