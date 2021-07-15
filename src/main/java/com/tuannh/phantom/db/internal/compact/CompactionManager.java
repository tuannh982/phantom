package com.tuannh.phantom.db.internal.compact;

import com.tuannh.phantom.db.index.IndexMetadata;
import com.tuannh.phantom.db.internal.DBDirectory;
import com.tuannh.phantom.db.internal.PhantomDBInternal;
import com.tuannh.phantom.db.internal.file.DBFile;
import com.tuannh.phantom.db.internal.file.Record;
import com.tuannh.phantom.db.internal.utils.CompactionUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

@Slf4j
public class CompactionManager implements Closeable {
    // primary
    private final DBDirectory dbDirectory;
    private PhantomDBInternal dbInternal;
    private final Map<Integer, Integer> tombstoneLastAssociateDataFileMap;
    // files
    private DBFile currentDBFile;
    // state
    private boolean started = false;
    private boolean closed = false;

    public CompactionManager(DBDirectory dbDirectory, Map<Integer, Integer> tombstoneLastAssociateDataFileMap) {
        this.dbDirectory = dbDirectory;
        this.tombstoneLastAssociateDataFileMap = tombstoneLastAssociateDataFileMap;
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
        // TODO start compaction thread
    }

    public void queueForCompaction(DBFile fileToCompact) {
        // TODO
    }

    private IndexMetadata writeToCurrentDBFile(Record entry) throws IOException {
        currentDBFile = dbInternal.rolloverDBFile(currentDBFile, entry);
        return currentDBFile.writeRecord(entry);
    }

    @Override
    public synchronized void close() throws IOException {
        if (!closed) {
            if (started) {
                // TODO impl
                started = false;
                closed = true;
            } else {
                log.warn("Compaction Manager not started yet");
            }
        } else {
            log.warn("Compaction Manager already closed");
        }
    }
}
