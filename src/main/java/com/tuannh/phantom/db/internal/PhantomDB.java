package com.tuannh.phantom.db.internal;

import com.tuannh.phantom.db.DB;
import com.tuannh.phantom.db.DBException;

import java.io.File;
import java.io.IOException;

public class PhantomDB implements DB {
    private final PhantomDBInternal internal;

    public PhantomDB(File dir, PhantomDBOptions options) throws IOException, DBException {
        if (!dir.isDirectory()) {
            throw new AssertionError(dir.getName() + " is not a directory");
        }
        internal = PhantomDBInternal.open(dir, options);
    }

    @Override
    public byte[] get(byte[] key) throws DBException, IOException {
        return internal.get(key);
    }

    @Override
    public boolean putIfAbsent(byte[] key, byte[] value) throws DBException, IOException {
        return internal.putIfAbsent(key, value);
    }

    @Override
    public boolean replace(byte[] key, byte[] value) throws DBException, IOException {
        return internal.replace(key, value);
    }

    @Override
    public void delete(byte[] key) throws DBException, IOException {
        internal.delete(key);
    }

    @Override
    public void close() throws IOException {
        internal.close();
    }
}
