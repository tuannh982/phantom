package com.tuannh.phantom.db.internal;

import com.tuannh.phantom.db.DB;
import com.tuannh.phantom.db.DBException;

import java.io.File;
import java.io.IOException;

public class PhantomDB implements DB {
    private final PhantomDBInternal internal;

    public PhantomDB(File dir, PhantomDBOptions options) throws IOException {
        if (!dir.isDirectory()) {
            throw new AssertionError(dir.getName() + " is not a directory");
        }
        internal = new PhantomDBInternal(dir, options);
    }

    @Override
    public byte[] get(byte[] key) throws DBException {
        return internal.get(key);
    }

    @Override
    public boolean put(byte[] key, byte[] value) throws DBException {
        return internal.put(key, value);
    }

    @Override
    public boolean delete(byte[] key) throws DBException {
        return internal.delete(key);
    }

    @Override
    public void close() throws IOException {
        internal.close();
    }
}
