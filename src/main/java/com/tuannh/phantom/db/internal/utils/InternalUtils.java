package com.tuannh.phantom.db.internal.utils;

import com.tuannh.phantom.db.index.IndexMap;
import com.tuannh.phantom.db.index.IndexMetadata;
import com.tuannh.phantom.db.internal.DBDirectory;
import com.tuannh.phantom.db.internal.PhantomDBOptions;
import com.tuannh.phantom.db.internal.file.*;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class InternalUtils {
    private static class IndexFileProcessTask implements Callable<Long> {
        private final IndexMap indexMap;
        private final Map<Integer, Integer> staleDataMap;
        private final IndexFile file;
        private final int fileId;

        public IndexFileProcessTask(IndexMap indexMap, Map<Integer, Integer> staleDataMap, IndexFile file) {
            this.indexMap = indexMap;
            this.staleDataMap = staleDataMap;
            this.file = file;
            this.fileId = file.getFileId();
        }

        @SuppressWarnings("java:S135")
        @Override
        public Long call() throws Exception {
            long maxSequenceNumber = Long.MIN_VALUE;
            Iterator<IndexFileEntry> iterator = file.iterator();
            while (iterator.hasNext()) {
                IndexFileEntry entry = iterator.next();
                // get
                byte[] key = entry.getKey();
                long sequenceNumber = entry.getHeader().getSequenceNumber();
                int recordOffset = entry.getHeader().getRecordOffset();
                int recordSize = entry.getHeader().getRecordSize();
                // calc
                int valueOffset = recordOffset + Record.HEADER_SIZE + key.length;
                int valueSize = recordSize - Record.HEADER_SIZE - key.length;
                IndexMetadata metadata = new IndexMetadata(fileId, valueOffset, valueSize, sequenceNumber);
                // update max sequence number.
                maxSequenceNumber = Math.max(maxSequenceNumber, sequenceNumber);
                // determine if it's old record
                if (indexMap.putIfAbsent(key, metadata) != null) {
                    while (true) {
                        /*
                        use while true since one metadata (existedMetadata, metadata) need to be evicted
                         */
                        IndexMetadata existedMetadata = indexMap.get(key);
                        if (existedMetadata.getSequenceNumber() > sequenceNumber) {
                            // stale data, add to stale map to compact later
                            recordStaleData(staleDataMap, fileId, recordSize);
                            break;
                        } else if (indexMap.replace(key, existedMetadata, metadata)) {
                            /*
                            use replace instead of compare existed metadata to current metadata, because
                            replace() method will block indexMap write methods (put(), putIfAbsent(), replace(),
                            so no other thread can interference here
                             */
                            // add current indexed data to stale map
                            int existedRecordSize = Record.HEADER_SIZE + key.length + existedMetadata.getValueSize();
                            recordStaleData(staleDataMap, existedMetadata.getFileId(), existedRecordSize);
                            break;
                        }
                    }
                }
            }
            file.close();
            return maxSequenceNumber;
        }
    }

    private static class TombstoneFileProcessTask implements Callable<Long> {
        private final IndexMap indexMap;
        private final Map<Integer, Integer> staleDataMap;
        private final TombstoneFile file;
        private final DBDirectory dbDirectory;
        private final PhantomDBOptions options;
        private final int fileId;

        public TombstoneFileProcessTask(
                IndexMap indexMap,
                Map<Integer, Integer> staleDataMap,
                TombstoneFile file,
                DBDirectory dbDirectory,
                PhantomDBOptions options
        ) {
            this.indexMap = indexMap;
            this.staleDataMap = staleDataMap;
            this.file = file;
            this.dbDirectory = dbDirectory;
            this.options = options;
            this.fileId = file.getFileId();
        }

        @Override
        public Long call() throws Exception {
            // prepare
            long maxSequenceNumber = Long.MIN_VALUE;
            Iterator<TombstoneFileEntry> iterator = file.iterator();
            // create new fresh tombstone file to write
            TombstoneFile compactedFile = null; // just lazy
            // do
            while (iterator.hasNext()) {
                TombstoneFileEntry entry = iterator.next();
                // get
                byte[] key = entry.getKey();
                long sequenceNumber = entry.getHeader().getSequenceNumber();
                // update max sequence number.
                maxSequenceNumber = Math.max(maxSequenceNumber, sequenceNumber);
                IndexMetadata existedMetadata = indexMap.get(key);
                if (existedMetadata != null && existedMetadata.getSequenceNumber() < sequenceNumber) {
                    indexMap.delete(key);
                    int existedRecordSize = Record.HEADER_SIZE + key.length + existedMetadata.getValueSize();
                    recordStaleData(staleDataMap, existedMetadata.getFileId(), existedRecordSize);
                    if (compactedFile == null) {
                        compactedFile = TombstoneFile.createCompacted(fileId, dbDirectory, options);
                    }
                    compactedFile.write(entry); // no need to rollover since compactedFile's size always smaller than old file size
                }
            }
            file.close();
            if (compactedFile != null) {
                compactedFile.flushToDisk();
                compactedFile.close();
                Files.move(compactedFile.path(), file.path(), REPLACE_EXISTING, ATOMIC_MOVE);
            }
            return maxSequenceNumber;
        }
    }

    public static void recordStaleData(Map<Integer, Integer> staleDataMap, int fileId, int recordSize) {
        Integer i = staleDataMap.get(fileId);
        int staleData = i == null ? 0 : i;
        staleDataMap.put(fileId, staleData + recordSize);
    }

    @SuppressWarnings({"java:S2142", "java:S3776"})
    public static Map.Entry<Integer, Long> buildInMemoryIndex(
            ExecutorService executorService,
            IndexMap indexMap,
            Map<Integer, Integer> staleDataMap,
            Map<Integer, Integer> tombstoneLastAssociateDataFileMap,
            int maxFileId,
            int maxDataFileId,
            DBDirectory dbDirectory,
            PhantomDBOptions options
    ) throws IOException {
        // ---------------------Prep   ----------------------------------------------------------------
        long maxSequenceNumber = Long.MIN_VALUE;
        int currentMaxFileId = maxFileId;
        // ---------------------STAGE 1 (process index file)-------------------------------------------
        File[] indexFiles = dbDirectory.indexFiles();
        int indexFileCount = indexFiles.length;
        List<IndexFileProcessTask> indexingTasks = new ArrayList<>(indexFileCount);
        for (File file : indexFiles) {
            int fileId = DirectoryUtils.fileId(file, DirectoryUtils.INDEX_FILE_PATTERN);
            IndexFile indexFile = IndexFile.open(fileId, dbDirectory, options);
            indexingTasks.add(new IndexFileProcessTask(indexMap, staleDataMap, indexFile));
        }
        try {
            List<Future<Long>> futures = executorService.invokeAll(indexingTasks);
            for (Future<Long> future : futures) {
                maxSequenceNumber = Math.max(maxSequenceNumber, future.get());
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException("indexing task interrupted", e);
        }
        // ---------------------STAGE 2 (process tombstone file)--------------------------------------
        File[] tombstoneFiles = dbDirectory.tombstoneFiles();
        int tombstoneFileCount = tombstoneFiles.length;
        List<TombstoneFileProcessTask> tombstoneFileProcessTasks = new ArrayList<>(tombstoneFileCount);
        for (File file : tombstoneFiles) {
            int fileId = DirectoryUtils.fileId(file, DirectoryUtils.TOMBSTONE_FILE_PATTERN);
            TombstoneFile tombstoneFile = TombstoneFile.open(fileId, dbDirectory, options);
            tombstoneFileProcessTasks.add(new TombstoneFileProcessTask(indexMap, staleDataMap, tombstoneFile, dbDirectory, options));
        }
        try {
            List<Future<Long>> futures = executorService.invokeAll(tombstoneFileProcessTasks);
            for (Future<Long> future : futures) {
                maxSequenceNumber = Math.max(maxSequenceNumber, future.get());
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException("tombstone process task interrupted", e);
        }
        // ---------------------STAGE 3 (merge tombstone file)----------------------------------------
        // re-scan tombstone file
        tombstoneFiles = dbDirectory.tombstoneFiles(); // already sorted
        tombstoneFileCount = tombstoneFiles.length;
        if (tombstoneFileCount > 0) {
            // construct tombstone list
            TombstoneFile[] tombstones = new TombstoneFile[tombstoneFileCount];
            for (int i = 0; i < tombstoneFileCount; i++) {
                File file = tombstoneFiles[i];
                int fileId = DirectoryUtils.fileId(file, DirectoryUtils.TOMBSTONE_FILE_PATTERN);
                tombstones[i] = TombstoneFile.open(fileId, dbDirectory, options);
            }
            // just write to new files
            TombstoneFile mergedTombstoneFile = null; // just lazy
            for (TombstoneFile tombstoneFile : tombstones) {
                Iterator<TombstoneFileEntry> iterator = tombstoneFile.iterator();
                while (iterator.hasNext()) {
                    TombstoneFileEntry entry = iterator.next();
                    // rollover current tombstone file
                    if (mergedTombstoneFile == null) {
                        // create new file
                        currentMaxFileId++;
                        mergedTombstoneFile = TombstoneFile.create(currentMaxFileId, dbDirectory, options);
                        dbDirectory.sync();
                    } else if (mergedTombstoneFile.getWriteOffset() + entry.serializedSize() > options.getMaxFileSize()) {
                        // flush current file
                        mergedTombstoneFile.flushToDisk();
                        mergedTombstoneFile.close();
                        tombstoneLastAssociateDataFileMap.put(mergedTombstoneFile.getFileId(), maxDataFileId);
                        // create new file
                        currentMaxFileId++;
                        mergedTombstoneFile = TombstoneFile.create(currentMaxFileId, dbDirectory, options);
                        dbDirectory.sync();
                    }
                    mergedTombstoneFile.write(entry);
                }
                if (mergedTombstoneFile != null) {
                    mergedTombstoneFile.flushToDisk(); // forced flush
                }
                tombstoneFile.close();
                tombstoneFile.delete();
            }
            if (mergedTombstoneFile != null) {
                tombstoneLastAssociateDataFileMap.put(mergedTombstoneFile.getFileId(), maxDataFileId);
            }
        }
        return new AbstractMap.SimpleImmutableEntry<>(currentMaxFileId, maxSequenceNumber);
    }
}
