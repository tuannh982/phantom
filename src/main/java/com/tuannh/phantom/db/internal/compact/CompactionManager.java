package com.tuannh.phantom.db.internal.compact;

import com.tuannh.phantom.db.index.IndexMetadata;
import com.tuannh.phantom.db.internal.DBDirectory;
import com.tuannh.phantom.db.internal.PhantomDBInternal;
import com.tuannh.phantom.db.internal.PhantomDBOptions;
import com.tuannh.phantom.db.internal.file.DBFile;
import com.tuannh.phantom.db.internal.file.IndexFileEntry;
import com.tuannh.phantom.db.internal.file.Record;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;

@Slf4j
public class CompactionManager implements Closeable {
    // primary
    private final DBDirectory dbDirectory;
    private final PhantomDBOptions options;
    private PhantomDBInternal dbInternal;
    private final Map<Integer, Integer> tombstoneLastAssociateDataFileMap;
    // current DB files (manual control write offset and unflush data)
    private int currentWriteOffset;
    private DBFile currentDBFile;
    private long unflushed;
    // queue
    private final BlockingQueue<DBFile> compactionQueue;
    // task
    private final ExecutorService compactionProcessor = Executors.newSingleThreadExecutor();
    // state
    private boolean started = false;
    private boolean closed = false;

    public CompactionManager(DBDirectory dbDirectory, PhantomDBOptions options, Map<Integer, Integer> tombstoneLastAssociateDataFileMap) {
        this.dbDirectory = dbDirectory;
        this.options = options;
        this.tombstoneLastAssociateDataFileMap = tombstoneLastAssociateDataFileMap;
        this.compactionQueue = new LinkedBlockingQueue<>();
    }

    public synchronized void start(PhantomDBInternal dbInternal) {
        if (started) {
            log.warn("Compaction Manager already started");
            return;
        }
        if (closed) {
            log.warn("Compaction Manager already closed");
        }
        this.dbInternal = dbInternal;
        started = true;
        compactionProcessor.submit(this::compactionJob);
    }

    public void queueForCompaction(DBFile fileToCompact) {
        if (started && !closed) {
            compactionQueue.offer(fileToCompact);
        }
    }

    @SuppressWarnings({"java:S2142", "java:S1141"})
    private void compactionJob() {
        while (started && !closed) {
            try {
                DBFile file = compactionQueue.poll(5, TimeUnit.SECONDS);
                if (file != null) {
                    try {
                        log.info("Compacting {}", file.file().getName());
                        doCompact(file);
                        log.info("Compaction on {}, done", file.file().getName());
                        dbInternal.markAsCompacted(file.getFileId());
                    } catch (IOException e) {
                        log.error("Error while compaction data file {}", file.file().getName(), e);
                    }
                }
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    private void doCompact(DBFile dbFile) throws IOException {
        FileChannel toBeCompactedFileChannel = dbFile.getChannel();
        Iterator<IndexFileEntry> iterator = dbFile.getIndexFile().iterator();
        int fileId = dbFile.getFileId();
        while (iterator.hasNext()) {
            IndexFileEntry entry = iterator.next();
            byte[] key = entry.getKey();
            int recordOffset = entry.getHeader().getRecordOffset();
            int recordSize = entry.getHeader().getRecordSize();
            int valueOffset = recordOffset + Record.HEADER_SIZE + entry.getHeader().getKeySize();
            int valueSize = recordSize - Record.HEADER_SIZE - entry.getHeader().getKeySize();
            long sequenceNumber = entry.getHeader().getSequenceNumber();
            IndexMetadata indexMetadata = dbInternal.getIndexMap().get(key);
            if (isFresh(indexMetadata, fileId, valueOffset, valueSize, sequenceNumber)) {
                rolloverCurrentDBFile(recordSize);
                int newRecordOffset = currentWriteOffset;
                // direct record data transfer (faster than just read record)
                long transferred = toBeCompactedFileChannel.transferTo(recordOffset, recordSize, currentDBFile.getChannel());
                unflushed += transferred;
                if (unflushed > options.getDataFlushThreshold()) {
                    currentDBFile.flush();
                    unflushed = 0;
                }
                // craft new index entry since the record offset changed
                IndexFileEntry newIndexEntry = new IndexFileEntry(key, newRecordOffset, recordSize);
                newIndexEntry.getHeader().setSequenceNumber(sequenceNumber);
                currentDBFile.getIndexFile().write(newIndexEntry);
                // craft new index metadata
                int newValueOffset = newRecordOffset + Record.HEADER_SIZE + key.length;
                IndexMetadata newIndexMetadata = new IndexMetadata(currentDBFile.getFileId(), newValueOffset, valueSize, sequenceNumber);
                boolean updated = dbInternal.getIndexMap().replace(key, indexMetadata, newIndexMetadata);
                if (!updated) {
                    dbInternal.markDataAsStale(currentDBFile.getFileId(), recordSize);
                }
                currentWriteOffset += recordSize;
            }
        }
        currentDBFile.flushToDisk();
        dbInternal.markAsCompacted(fileId);
        // TODO delete tombstone file if tombstoneLastAssociateDataFileMap condition satisfied
    }

    private boolean isFresh(IndexMetadata indexMetadata, int fileId, int valueOffset, int valueSize, long sequenceNumber) {
        if (indexMetadata != null) {
            return
                    indexMetadata.getFileId() == fileId &&
                    indexMetadata.getValueOffset() == valueOffset &&
                    indexMetadata.getValueSize() == valueSize &&
                    indexMetadata.getSequenceNumber() == sequenceNumber;
        } else {
            return false;
        }
    }

    private void rolloverCurrentDBFile(int recordSize) throws IOException {
        if (currentDBFile == null) {
            unflushed = 0;
            currentWriteOffset = 0;
            currentDBFile = dbInternal.createNewDBFile();
            dbDirectory.sync();
        } else if (currentWriteOffset + recordSize > options.getMaxFileSize()) {
            currentDBFile.close();
            unflushed = 0;
            currentWriteOffset = 0;
            currentDBFile = dbInternal.createNewDBFile();
            dbDirectory.sync();
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (!closed) {
            if (started) {
                started = false;
                closed = true;
                compactionProcessor.shutdown();
                if (currentDBFile != null) {
                    currentDBFile.close();
                }
            } else {
                log.warn("Compaction Manager not started yet");
            }
        } else {
            log.warn("Compaction Manager already closed");
        }
    }
}
