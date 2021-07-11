package com.tuannh.phantom.db.internal;

import com.tuannh.phantom.commons.concurrent.RLock;
import com.tuannh.phantom.commons.io.FileUtils;
import com.tuannh.phantom.db.DBException;
import com.tuannh.phantom.db.internal.utils.DirectoryUtils;
import com.tuannh.phantom.offheap.hashtable.KeyBuffer;
import com.tuannh.phantom.offheap.hashtable.hash.Hasher;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class PhantomDBInternal implements Closeable {
    private final DBDirectory dbDirectory;
    private final RLock writeLock;
    private final Hasher hasher;

    public PhantomDBInternal(File dir, PhantomDBOptions options) throws IOException {
        hasher = options.getHasher();
        writeLock = new RLock();
        dbDirectory = new DBDirectory(dir);
    }

    public byte[] get(byte[] key) throws DBException {
        KeyBuffer keyBuffer = new KeyBuffer(key, hasher.hash(key));
        return new byte[0]; // TODO
    }

    public boolean put(byte[] key, byte[] value) throws DBException {
        boolean rlock = writeLock.lock();
        try {
            KeyBuffer keyBuffer = new KeyBuffer(key, hasher.hash(key));
            // TODO
        } finally {
            writeLock.release(rlock);
        }
        return false; // TODO
    }

    public boolean delete(byte[] key) throws DBException {
        boolean rlock = writeLock.lock();
        try {
            KeyBuffer keyBuffer = new KeyBuffer(key, hasher.hash(key));
            // TODO
        } finally {
            writeLock.release(rlock);
        }
        return false; // TODO
    }

    public void snapshot(File snapshotDir) throws IOException {
        // TODO force compaction & merge tombstone files
        File[] files = FileUtils.ls(dbDirectory.file(), DirectoryUtils.STORAGE_FILE_PATTERN);
        for (File file : files) {
            Path dest = Paths.get(snapshotDir.getAbsolutePath(), file.getName());
            FileUtils.copy(file.toPath(), dest);
        }
    }

    public void close() throws IOException {
        // TODO
    }
}
