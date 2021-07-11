package com.tuannh.phantom.db.internal;

import com.tuannh.phantom.db.DBException;
import com.tuannh.phantom.offheap.hashtable.KeyBuffer;
import com.tuannh.phantom.offheap.hashtable.hash.Hasher;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

public class PhantomDBInternal implements Closeable {
    private final Hasher hasher;

    public PhantomDBInternal(File dir, PhantomDBOptions options) {
        hasher = options.getHasher();
    }

    public byte[] get(byte[] key) throws DBException {
        KeyBuffer keyBuffer = new KeyBuffer(key, hasher.hash(key));
        return new byte[0]; // TODO
    }

    public boolean put(byte[] key, byte[] value) throws DBException {
        KeyBuffer keyBuffer = new KeyBuffer(key, hasher.hash(key));
        return false; // TODO
    }

    public boolean delete(byte[] key) throws DBException {
        KeyBuffer keyBuffer = new KeyBuffer(key, hasher.hash(key));
        return false; // TODO
    }

    public void close() throws IOException {
        // TODO
    }
}
