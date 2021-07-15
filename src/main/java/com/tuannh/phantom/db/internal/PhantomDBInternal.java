package com.tuannh.phantom.db.internal;

import com.tuannh.phantom.commons.concurrent.RLock;
import com.tuannh.phantom.commons.io.FileUtils;
import com.tuannh.phantom.db.DBException;
import com.tuannh.phantom.db.index.IndexMap;
import com.tuannh.phantom.db.index.IndexMetadata;
import com.tuannh.phantom.db.index.OnHeapInMemoryIndex;
import com.tuannh.phantom.db.internal.file.DBFile;
import com.tuannh.phantom.db.internal.file.Record;
import com.tuannh.phantom.db.internal.file.TombstoneFile;
import com.tuannh.phantom.db.internal.file.TombstoneFileEntry;
import com.tuannh.phantom.db.internal.utils.DirectoryUtils;
import com.tuannh.phantom.db.internal.utils.InternalUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
    private final IndexMap indexMap;
    // compaction
    private final Map<Integer, Integer> staleDataMap;
    // lock
    private final RLock writeLock;
    //
    private long sequenceNumber = 0;
    private int fileId;

    @SuppressWarnings({"java:S4042", "java:S899", "ResultOfMethodCallIgnored"})
    public static PhantomDBInternal open(File dir, PhantomDBOptions options) throws IOException, DBException {
        log.info("initiating DBDirectory...");
        // db directory
        DBDirectory dbDirectory = new DBDirectory(dir);
        log.info("DBDirectory initiated");
        // write lock
        RLock writeLock = new RLock();
        log.info("Building data files map...");
        // index
        IndexMap indexMap = new OnHeapInMemoryIndex(); // FIXME Just for testing purpose
        Map.Entry<Map<Integer, DBFile>, Integer> dataFileMapReturn = DirectoryUtils.buildDataFileMap(dbDirectory, options, 1);
        // max data file id (for tombstone compaction)
        int maxDataFileId = dataFileMapReturn.getValue();
        // data map (for data query)
        Map<Integer, DBFile> dataFileMap = dataFileMapReturn.getKey();
        log.info("Data files map built");
        log.info("Deleting orphaned index files...");
        // delete all orphaned index files (never happen, but still process just for sure)
        DirectoryUtils.deleteOrphanedIndexFiles(dataFileMap, dbDirectory);
        log.info("Orphaned index files deleted");
        log.info("Loading DBMetadata...");
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
            // no compacted tombstone should be exists, so just delete all
            File[] compactedTombstoneFiles = DirectoryUtils.compactedTombstoneFiles(dbDirectory.file());
            for (File file : compactedTombstoneFiles) {
                file.delete();
            }
        }
        // reset metadata
        dbMetadata.setOpen(true);
        dbMetadata.setIoError(false);
        dbMetadata.setMaxFileSize(options.getMaxFileSize());
        // save & reload directory
        dbMetadata.save(dbDirectory);
        log.info("DBMetadata loaded");
        // staleMap (for compaction)
        Map<Integer, Integer> staleDataMap = new ConcurrentHashMap<>();
        /*
        last associate data file map, if no data file with file_id <= last associate data file id exists
        -> delete tombstone file since all associated files have been deleted already (they're compacted)
         */
        Map<Integer, Integer> tombstoneLastAssociateDataFileMap = new ConcurrentHashMap<>();
        // indexing
        long maxSequenceNumber = Long.MIN_VALUE;
        // max file id (to generate new fileId)
        int maxFileId = dbDirectory.maxFileId(); // maxFileId across all storage files
        ExecutorService indexingProcessor = Executors.newFixedThreadPool(options.getNumberOfIndexingThread());
        log.info("Indexing Processor initiated with {} thread(s)", options.getNumberOfIndexingThread());
        try {
            log.info("Building index...");
            Map.Entry<Integer, Long> indexBuildReturn = InternalUtils.buildInMemoryIndex(
                    indexingProcessor, indexMap,
                    staleDataMap, tombstoneLastAssociateDataFileMap,
                    maxFileId, maxDataFileId,
                    dbDirectory, options
            );
            maxFileId = indexBuildReturn.getKey();
            maxSequenceNumber = indexBuildReturn.getValue();
        } finally {
            indexingProcessor.shutdown();
            log.info("Index built");
        }
        // TODO initiate compaction manager
        // TODO impl
    }

    @SuppressWarnings("java:S1168") // no result return null, not empty array
    public byte[] get(byte[] key) throws DBException, IOException {
        IndexMetadata metadata = indexMap.get(key);
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
            IndexMetadata metadata = indexMap.get(key);
            if (metadata == null) {
                Record entry = new Record(key, value);
                entry.getHeader().setSequenceNumber(nextSequenceNumber());
                IndexMetadata indexMetadata = writeToCurrentDBFile(entry);
                indexMap.put(key, indexMetadata);
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
            IndexMetadata existedMetadata = indexMap.get(key);
            if (existedMetadata == null) {
                return false; // not exists
            } else {
                Record entry = new Record(key, value);
                entry.getHeader().setSequenceNumber(nextSequenceNumber());
                IndexMetadata indexMetadata = writeToCurrentDBFile(entry);
                indexMap.put(key, indexMetadata);
                // mark previous version as stale data
                int existedRecordSize = Record.HEADER_SIZE + key.length + existedMetadata.getValueSize();
                InternalUtils.recordStaleData(staleDataMap, existedMetadata.getFileId(), existedRecordSize);
                return true;
            }
        } finally {
            writeLock.release(rlock);
        }
    }

    public void delete(byte[] key) throws DBException, IOException {
        boolean rlock = writeLock.lock();
        try {
            IndexMetadata existedMetadata = indexMap.get(key);
            if (existedMetadata != null) {
                indexMap.delete(key);
                TombstoneFileEntry entry = new TombstoneFileEntry(key);
                entry.getHeader().setSequenceNumber(nextSequenceNumber());
                writeToCurrentTombstoneFile(entry);
                // mark previous version as stale data
                int existedRecordSize = Record.HEADER_SIZE + key.length + existedMetadata.getValueSize();
                InternalUtils.recordStaleData(staleDataMap, existedMetadata.getFileId(), existedRecordSize);
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
        sequenceNumber++;
        if (sequenceNumber == Long.MIN_VALUE) {
            return ++sequenceNumber;
        } else {
            return sequenceNumber;
        }
    }

    public long nextFileId() {
        fileId++;
        if (fileId == Integer.MIN_VALUE) {
            return ++fileId;
        } else {
            return fileId;
        }
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
            indexMap.close();
            // TODO impl
        } finally {
            writeLock.release(rlock);
        }
    }
}
