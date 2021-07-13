package com.tuannh.phantom.db.internal;

import com.tuannh.phantom.commons.concurrent.RLock;
import com.tuannh.phantom.commons.io.FileUtils;
import com.tuannh.phantom.db.DBException;
import com.tuannh.phantom.db.index.InMemoryIndex;
import com.tuannh.phantom.db.index.IndexMetadata;
import com.tuannh.phantom.db.index.OnHeapInMemoryIndex;
import com.tuannh.phantom.db.internal.file.DBFile;
import com.tuannh.phantom.db.internal.file.Record;
import com.tuannh.phantom.db.internal.file.TombstoneFile;
import com.tuannh.phantom.db.internal.file.TombstoneFileEntry;
import com.tuannh.phantom.db.internal.utils.DirectoryUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

@Slf4j
public class PhantomDBInternal implements Closeable { // TODO add compaction manager, tombstone file merger (tombstone compaction)
    // primary
    private final DBDirectory dbDirectory;
    private final DBMetadata dbMetadata;
    private final Map<Integer, DBFile> dataFileMap;
    // files
    private DBFile currentDBFile;
    private TombstoneFile currentTombstoneFile;
    // index
    private final InMemoryIndex inMemoryIndex;
    // lock
    private final RLock writeLock;
    //
    private long sequenceNumber = 0;

    public static PhantomDBInternal open(File dir, PhantomDBOptions options) throws IOException, DBException {
        // db directory
        DBDirectory dbDirectory = new DBDirectory(dir);
        // write lock
        RLock writeLock = new RLock();
        // index
        InMemoryIndex inMemoryIndex = new OnHeapInMemoryIndex(); // Just for testing purpose
        Map.Entry<Map<Integer, DBFile>, Integer> dataFileMapReturn = DirectoryUtils.buildDataFileMap(dbDirectory, options, 1);
        // max file id (for generating new files)
        int maxFileId = dataFileMapReturn.getValue();
        // data map (for data query)
        Map<Integer, DBFile> dataFileMap = dataFileMapReturn.getKey();
        // load db metadata
        DBMetadata dbMetadata = DBMetadata.load(dbDirectory, new DBMetadata());
        if (dbMetadata.getMaxFileSize() != options.getMaxFileSize()) {
            throw new DBException("could not change max_file_size after DB was created");
        }
        if (dbMetadata.isOpen() || dbMetadata.isIoError()) { // last open
            log.warn("DB was not correctly shutdown last time. Trying to repair data files...");
            // only have to repair the last data file (one data file, one compacted file)
            DirectoryUtils.repairLatestDataFile(dataFileMap);
            // repair last tombstone file
            DirectoryUtils.repairLatestTombstoneFile(dbDirectory, options);
        }
        dbMetadata.setOpen(true);
        dbMetadata.setIoError(false);
        dbMetadata.setMaxFileSize(options.getMaxFileSize());
        dbMetadata.save(dbDirectory);
        // TODO impl
    }

    public byte[] get(byte[] key) throws DBException, IOException {
        IndexMetadata metadata = inMemoryIndex.get(key);
        if (metadata == null) {
            return null;
        } else {
            DBFile file = dataFileMap.get(metadata.getFileId());
            if (file == null) {
                return null;
            } else {
                return file.read(metadata.getValueOffset(), metadata.getValueSize());
            }
        }
    }

    public boolean putIfAbsent(byte[] key, byte[] value) throws DBException, IOException {
        boolean rlock = writeLock.lock();
        try {
            IndexMetadata metadata = inMemoryIndex.get(key);
            if (metadata == null) {
                Record entry = new Record(key, value);
                entry.getHeader().setSequenceNumber(nextSequenceNumber());
                IndexMetadata indexMetadata = writeToCurrentDBFile(entry);
                inMemoryIndex.put(key, indexMetadata);
                return true;
            } else {
                return false; // already exists
            }
        } finally {
            writeLock.release(rlock);
        }
    }

    public boolean replace(byte[] key, byte[] value) throws DBException, IOException {
        boolean rlock = writeLock.lock();
        try {
            IndexMetadata metadata = inMemoryIndex.get(key);
            if (metadata == null) {
                return false; // not exists
            } else {
                Record entry = new Record(key, value);
                entry.getHeader().setSequenceNumber(nextSequenceNumber());
                IndexMetadata indexMetadata = writeToCurrentDBFile(entry);
                inMemoryIndex.put(key, indexMetadata);
                markPreviousVersionAsStale(key, metadata); // TODO impl
                return true;
            }
        } finally {
            writeLock.release(rlock);
        }
    }

    public void delete(byte[] key) throws DBException, IOException {
        boolean rlock = writeLock.lock();
        try {
            IndexMetadata metadata = inMemoryIndex.get(key);
            if (metadata != null) {
                inMemoryIndex.delete(key);
                TombstoneFileEntry entry = new TombstoneFileEntry(key);
                entry.getHeader().setSequenceNumber(nextSequenceNumber());
                writeToCurrentTombstoneFile(entry);
                markPreviousVersionAsStale(key, metadata); // TODO impl
            }
        } finally {
            writeLock.release(rlock);
        }
    }

    public void snapshot(File snapshotDir) throws IOException {
        // TODO force compaction & merge tombstone files
        File[] files = FileUtils.ls(dbDirectory.file(), DirectoryUtils.STORAGE_FILE_PATTERN);
        for (File file : files) {
            Path dest = Paths.get(snapshotDir.getAbsolutePath(), file.getName());
            FileUtils.copy(file.toPath(), dest);
        }
    }

    public long nextSequenceNumber() {
        return ++sequenceNumber;
    }

    private IndexMetadata writeToCurrentDBFile(Record entry) throws IOException {
        rolloverCurrentDBFile(entry); // TODO impl
        return currentDBFile.writeRecord(entry);
    }

    private void writeToCurrentTombstoneFile(TombstoneFileEntry entry) throws IOException {
        rolloverCurrentTombstoneFile(entry); // TODO impl
        currentTombstoneFile.write(entry);
    }

    public void close() throws IOException {
        boolean rlock = writeLock.lock();
        try {
            inMemoryIndex.close();
            // TODO impl
        } finally {
            writeLock.release(rlock);
        }
    }
}
