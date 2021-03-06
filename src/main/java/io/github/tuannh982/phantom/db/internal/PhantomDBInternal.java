package io.github.tuannh982.phantom.db.internal;

import io.github.tuannh982.phantom.commons.concurrent.RLock;
import io.github.tuannh982.phantom.db.command.GetResult;
import io.github.tuannh982.phantom.db.command.ModifyResult;
import io.github.tuannh982.phantom.db.index.offheap.OffHeapInMemoryIndex;
import io.github.tuannh982.phantom.db.internal.file.Record;
import io.github.tuannh982.phantom.db.internal.file.TombstoneFile;
import io.github.tuannh982.phantom.db.internal.file.TombstoneFileEntry;
import io.github.tuannh982.phantom.db.internal.utils.DirectoryUtils;
import io.github.tuannh982.phantom.db.internal.utils.InternalUtils;
import io.github.tuannh982.phantom.db.DBException;
import io.github.tuannh982.phantom.db.index.IndexMap;
import io.github.tuannh982.phantom.db.index.IndexMetadata;
import io.github.tuannh982.phantom.db.index.OnHeapInMemoryIndex;
import io.github.tuannh982.phantom.db.internal.compact.CompactionManager;
import io.github.tuannh982.phantom.db.internal.file.DBFile;
import io.github.tuannh982.phantom.db.internal.utils.CompactionUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class PhantomDBInternal implements Closeable {
    // primary
    private final DBDirectory dbDirectory;
    @Getter
    private final PhantomDBOptions options;
    private final DBMetadata dbMetadata;
    // stale data
    private final Map<Integer, Integer> tombstoneLastAssociateDataFileMap;
    private final Map<Integer, Integer> staleDataMap;
    private final NavigableMap<Integer, DBFile> dataFileMap;
    // files
    private DBFile currentDBFile;
    private TombstoneFile currentTombstoneFile;
    // index
    @Getter
    private final IndexMap indexMap;
    // compaction
    @Getter
    private final CompactionManager compactionManager;
    private boolean currentDBFileWillBeCompactedLater = false;
    // lock
    private final RLock writeLock;
    //
    private long sequenceNumber;
    private int fileId;

    @SuppressWarnings({"java:S3776", "java:S4042", "java:S899", "java:S2095"})
    public static PhantomDBInternal open(File dir, PhantomDBOptions options) throws IOException, DBException {
        log.info("Initiating DBDirectory...");
        // db directory
        DBDirectory dbDirectory = new DBDirectory(dir);
        log.info("DBDirectory initiated");
        // write lock
        RLock writeLock = new RLock();
        log.info("Building data files map...");
        // index
        IndexMap indexMap = null;
        if (options.isOffHeapHashTable()) {
            indexMap = new OffHeapInMemoryIndex(
                    options.getEstimatedMaxKeyCount(),
                    options.getMaxKeySize(),
                    options.getMemoryChunkSize()
            );
        } else {
            indexMap = new OnHeapInMemoryIndex();
        }
        Map.Entry<NavigableMap<Integer, DBFile>, Integer> dataFileMapReturn = DirectoryUtils.buildDataFileMap(dbDirectory, options, 0);
        // max data file id (for tombstone compaction)
        int maxDataFileId = dataFileMapReturn.getValue();
        // data map (for data query)
        NavigableMap<Integer, DBFile> dataFileMap = dataFileMapReturn.getKey();
        log.info("Data files map built");
        log.info("Deleting orphaned index files...");
        // delete all orphaned index files (never happen, but still process just for sure)
        DirectoryUtils.deleteOrphanedIndexFiles(dataFileMap, dbDirectory);
        log.info("Orphaned index files deleted");
        log.info("Loading DBMetadata...");
        // load db metadata
        DBMetadata dbMetadata = DBMetadata.load(dbDirectory, null);
        if (dbMetadata == null) {
            dbMetadata = new DBMetadata();
        } else {
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
                    boolean b = file.delete();
                    if (!b) {
                        log.error("fail to delete file " + file.getName());
                    }
                }
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
        maxFileId = Math.max(maxFileId, maxDataFileId);
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
            if (maxSequenceNumber == Long.MIN_VALUE) {
                maxSequenceNumber = 0;
            }
        } finally {
            indexingProcessor.shutdown();
            log.info("Index built");
            log.info("Max fileId = {}, max sequenceNumber = {}", maxFileId, maxSequenceNumber);
        }
        log.info("Initiating Compaction Manager...");
        CompactionManager compactionManager = new CompactionManager(dbDirectory, options);
        log.info("Compaction Manager initiated");
        PhantomDBInternal dbInternal = new PhantomDBInternal(
                dbDirectory,
                options,
                dbMetadata,
                tombstoneLastAssociateDataFileMap, staleDataMap,
                dataFileMap,
                indexMap,
                compactionManager,
                writeLock,
                maxSequenceNumber + 1,
                maxFileId + 1
        );
        log.info("Submitting files to Compaction Manager");
        Iterator<Map.Entry<Integer, Integer>> staleDataMapIterator = staleDataMap.entrySet().iterator();
        int toBeCompactedFileCount = 0;
        while (staleDataMapIterator.hasNext()) {
            Map.Entry<Integer, Integer> entry = staleDataMapIterator.next();
            int toBeCompactedFileId = entry.getKey();
            int toBeCompactedStaleSize = entry.getValue();
            DBFile toBeCompactedFile = dataFileMap.get(toBeCompactedFileId);
            if (toBeCompactedFile == null) {
                staleDataMapIterator.remove();
            } else if (toBeCompactedStaleSize > options.getCompactionThreshold() * toBeCompactedFile.getWriteOffset()) { // write offset equals to file size
                compactionManager.queueForCompaction(toBeCompactedFile);
                toBeCompactedFileCount++;
                staleDataMapIterator.remove();
            }
        }
        log.info("{} files submitted to Compaction Manager", toBeCompactedFileCount);
        // TODO
        log.info("Starting Compaction Manager...");
        compactionManager.start(dbInternal);
        log.info("Compaction Manager started");
        return dbInternal;
    }

    @SuppressWarnings("java:S107")
    private PhantomDBInternal(
            DBDirectory dbDirectory,
            PhantomDBOptions options,
            DBMetadata dbMetadata,
            Map<Integer, Integer> tombstoneLastAssociateDataFileMap,
            Map<Integer, Integer> staleDataMap,
            NavigableMap<Integer, DBFile> dataFileMap,
            IndexMap indexMap,
            CompactionManager compactionManager,
            RLock writeLock,
            long sequenceNumber,
            int fileId) {
        this.dbDirectory = dbDirectory;
        this.options = options;
        this.dbMetadata = dbMetadata;
        this.tombstoneLastAssociateDataFileMap = tombstoneLastAssociateDataFileMap;
        this.staleDataMap = staleDataMap;
        this.dataFileMap = dataFileMap;
        this.indexMap = indexMap;
        this.compactionManager = compactionManager;
        this.writeLock = writeLock;
        this.sequenceNumber = sequenceNumber;
        this.fileId = fileId;
    }

    @SuppressWarnings("java:S1168") // no result return null, not empty array
    public GetResult get(byte[] key) throws IOException {
        IndexMetadata metadata = indexMap.get(key);
        if (metadata == null) {
            return GetResult.NULL;
        } else {
            DBFile file = dataFileMap.get(metadata.getFileId());
            if (file == null) {
                return GetResult.NULL;
            } else {
                byte[] readValue = file.read(metadata.getValueOffset(), metadata.getValueSize());
                return new GetResult(readValue, metadata.getSequenceNumber());
            }
        }
    }

    public ModifyResult put(byte[] key, byte[] value) throws IOException {
        boolean rlock = writeLock.lock();
        try {
            IndexMetadata existedMetadata = indexMap.get(key);
            if (existedMetadata != null) {
                // mark previous version as stale data
                markDataAsStale(key, existedMetadata);
            }
            Record entry = new Record(key, value);
            long toBeWrittenSequenceNumber = nextSequenceNumber();
            entry.getHeader().setSequenceNumber(toBeWrittenSequenceNumber);
            IndexMetadata indexMetadata = writeToCurrentDBFile(entry);
            indexMap.put(key, indexMetadata);
            return new ModifyResult(true, toBeWrittenSequenceNumber);
        } finally {
            writeLock.release(rlock);
        }
    }

    public ModifyResult putIfAbsent(byte[] key, byte[] value) throws IOException {
        boolean rlock = writeLock.lock();
        try {
            IndexMetadata metadata = indexMap.get(key);
            if (metadata == null) {
                Record entry = new Record(key, value);
                long toBeWrittenSequenceNumber = nextSequenceNumber();
                entry.getHeader().setSequenceNumber(toBeWrittenSequenceNumber);
                IndexMetadata indexMetadata = writeToCurrentDBFile(entry);
                indexMap.putIfAbsent(key, indexMetadata);
                return new ModifyResult(true, toBeWrittenSequenceNumber);
            } else {
                return new ModifyResult(false, metadata.getSequenceNumber()); // already exists
            }
        } finally {
            writeLock.release(rlock);
        }
    }

    public ModifyResult replace(byte[] key, byte[] value) throws IOException {
        boolean rlock = writeLock.lock();
        try {
            IndexMetadata existedMetadata = indexMap.get(key);
            if (existedMetadata == null) {
                return ModifyResult.FAILED; // not exists
            } else {
                Record entry = new Record(key, value);
                long toBeWrittenSequenceNumber = nextSequenceNumber();
                entry.getHeader().setSequenceNumber(toBeWrittenSequenceNumber);
                IndexMetadata indexMetadata = writeToCurrentDBFile(entry);
                indexMap.replace(key, existedMetadata, indexMetadata);
                // mark previous version as stale data
                markDataAsStale(key, existedMetadata);
                return new ModifyResult(true, toBeWrittenSequenceNumber);
            }
        } finally {
            writeLock.release(rlock);
        }
    }

    // for read-modify-write pattern
    public ModifyResult replaceWithSequenceNumberEquals(byte[] key, byte[] value, long sequenceNumber) throws IOException {
        boolean rlock = writeLock.lock();
        try {
            IndexMetadata existedMetadata = indexMap.get(key);
            if (existedMetadata == null) {
                return ModifyResult.FAILED; // not exists
            } else if (existedMetadata.getSequenceNumber() != sequenceNumber) {
                return new ModifyResult(false, existedMetadata.getSequenceNumber()); // sequence number not matched
            } else {
                Record entry = new Record(key, value);
                long toBeWrittenSequenceNumber = nextSequenceNumber();
                entry.getHeader().setSequenceNumber(toBeWrittenSequenceNumber);
                IndexMetadata indexMetadata = writeToCurrentDBFile(entry);
                indexMap.replace(key, existedMetadata, indexMetadata);
                // mark previous version as stale data
                markDataAsStale(key, existedMetadata);
                return new ModifyResult(true, toBeWrittenSequenceNumber);
            }
        } finally {
            writeLock.release(rlock);
        }
    }

    public ModifyResult delete(byte[] key) throws IOException {
        boolean rlock = writeLock.lock();
        try {
            IndexMetadata existedMetadata = indexMap.get(key);
            if (existedMetadata != null) {
                indexMap.delete(key);
                TombstoneFileEntry entry = new TombstoneFileEntry(key);
                long toBeWrittenSequenceNumber = nextSequenceNumber();
                entry.getHeader().setSequenceNumber(toBeWrittenSequenceNumber);
                writeToCurrentTombstoneFile(entry);
                // mark previous version as stale data
                markDataAsStale(key, existedMetadata);
                return new ModifyResult(true, toBeWrittenSequenceNumber);
            } else {
                return ModifyResult.FAILED;
            }
        } finally {
            writeLock.release(rlock);
        }
    }

    // for read-modify-write pattern
    public ModifyResult deleteWithSequenceNumberEquals(byte[] key, long sequenceNumber) throws IOException {
        boolean rlock = writeLock.lock();
        try {
            IndexMetadata existedMetadata = indexMap.get(key);
            if (existedMetadata != null && existedMetadata.getSequenceNumber() == sequenceNumber) {
                indexMap.delete(key);
                TombstoneFileEntry entry = new TombstoneFileEntry(key);
                long toBeWrittenSequenceNumber = nextSequenceNumber();
                entry.getHeader().setSequenceNumber(toBeWrittenSequenceNumber);
                writeToCurrentTombstoneFile(entry);
                // mark previous version as stale data
                markDataAsStale(key, existedMetadata);
                return new ModifyResult(true, toBeWrittenSequenceNumber);
            } else {
                return ModifyResult.FAILED;
            }
        } finally {
            writeLock.release(rlock);
        }
    }

    public long nextSequenceNumber() {
        return sequenceNumber++;
    }

    public int nextFileId() {
        return fileId++;
    }

    private IndexMetadata writeToCurrentDBFile(Record entry) throws IOException {
        rolloverCurrentDBFile(entry);
        return currentDBFile.writeRecord(entry);
    }

    private void writeToCurrentTombstoneFile(TombstoneFileEntry entry) throws IOException {
        rolloverCurrentTombstoneFile(entry);
        currentTombstoneFile.write(entry);
    }

    public DBFile createNewDBFile() throws IOException {
        DBFile file = DBFile.create(nextFileId(), dbDirectory, options, false);
        if (dataFileMap.putIfAbsent(file.getFileId(), file) != null) {
            throw new IOException("File already existed");
        }
        return file;
    }

    private void rolloverCurrentDBFile(Record entry) throws IOException {
        if (currentDBFile == null) {
            currentDBFile = createNewDBFile();
            if (currentTombstoneFile != null) {
                tombstoneLastAssociateDataFileMap.put(currentTombstoneFile.getFileId(), currentDBFile.getFileId());
            }
            dbDirectory.sync();
        } else if (currentDBFile.getWriteOffset() + entry.serializedSize() > options.getMaxFileSize()) {
            currentDBFile.flushToDisk();
            if (currentDBFileWillBeCompactedLater) {
                compactionManager.queueForCompaction(currentDBFile);
                currentDBFileWillBeCompactedLater = false;
            }
            currentDBFile = createNewDBFile();
            if (currentTombstoneFile != null) {
                tombstoneLastAssociateDataFileMap.put(currentTombstoneFile.getFileId(), currentDBFile.getFileId());
            }
            dbDirectory.sync();
        }
    }

    private void rolloverCurrentTombstoneFile(TombstoneFileEntry entry) throws IOException {
        if (currentTombstoneFile == null) {
            currentTombstoneFile = TombstoneFile.create(nextFileId(), dbDirectory, options);
            if (currentDBFile != null) {
                tombstoneLastAssociateDataFileMap.put(currentTombstoneFile.getFileId(), currentDBFile.getFileId());
            }
            dbDirectory.sync();
        } else if (currentTombstoneFile.getWriteOffset() + entry.serializedSize() > options.getMaxTombstoneFileSize()) {
            currentTombstoneFile.close();
            currentTombstoneFile = TombstoneFile.create(nextFileId(), dbDirectory, options);
            if (currentDBFile != null) {
                tombstoneLastAssociateDataFileMap.put(currentTombstoneFile.getFileId(), currentDBFile.getFileId());
            }
            dbDirectory.sync();
        }
    }

    public void markDataAsStale(int fileId, int staleSize) {
        DBFile toBeCompactedFile = dataFileMap.get(fileId);
        if (toBeCompactedFile == null) {
            staleDataMap.remove(fileId);
        } else if (staleSize > options.getCompactionThreshold() * toBeCompactedFile.getWriteOffset()) { // write offset equals to file size
            if (fileId != currentDBFile.getFileId()) {
                compactionManager.queueForCompaction(toBeCompactedFile);
            } else {
                currentDBFileWillBeCompactedLater = true;
            }
            staleDataMap.remove(fileId);
        }
    }

    public void markDataAsStale(byte[] key, IndexMetadata existedMetadata) {
        int existedRecordSize = Record.HEADER_SIZE + key.length + existedMetadata.getValueSize();
        int staleSize = CompactionUtils.recordStaleData(staleDataMap, existedMetadata.getFileId(), existedRecordSize);
        int staleFileId = existedMetadata.getFileId();
        markDataAsStale(staleFileId, staleSize);
    }

    public void markAsCompacted(int fileId) {
        staleDataMap.remove(fileId);
        DBFile file = dataFileMap.get(fileId);
        if (file != null) {
            file.delete();
        }
        dataFileMap.remove(fileId);
    }

    public void deleteOrphanedTombstone() throws IOException {
        Iterator<Map.Entry<Integer, Integer>> iterator = tombstoneLastAssociateDataFileMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Integer, Integer> entry = iterator.next();
            int tombstoneFileId = entry.getKey();
            int lastAssociatedDataFile = entry.getValue();
            if (dataFileMap.floorKey(lastAssociatedDataFile) == null) { // all data files with id <= lastAssociatedDataFile was compacted
                Files.deleteIfExists(dbDirectory.path().resolve(tombstoneFileId + TombstoneFile.TOMBSTONE_FILE_EXTENSION));
                log.info("Tombstone file {} deleted", tombstoneFileId);
                iterator.remove();
            }
        }
    }

    public void close() throws IOException {
        boolean rlock = writeLock.lock();
        try {
            compactionManager.close();
            if (currentDBFile != null) {
                currentDBFile.close();
            }
            if (currentTombstoneFile != null) {
                currentTombstoneFile.close();
            }
            for (DBFile dbFile : dataFileMap.values()) {
                dbFile.close();
            }
            dbDirectory.close();
            dbMetadata.setOpen(false);
            dbMetadata.setIoError(false);
            dbMetadata.save(dbDirectory);
            if (!options.isOffHeapHashTable()) {
                indexMap.close();
            }
        } finally {
            writeLock.release(rlock);
        }
    }
}
