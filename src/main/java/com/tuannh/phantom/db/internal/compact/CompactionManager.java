package com.tuannh.phantom.db.internal.compact;

import com.tuannh.phantom.db.index.IndexMetadata;
import com.tuannh.phantom.db.internal.file.DBFile;
import com.tuannh.phantom.db.internal.file.Record;
import com.tuannh.phantom.db.internal.utils.CompactionUtils;

import java.io.IOException;
import java.util.Map;

public class CompactionManager {
    // stale data
    private final Map<Integer, Integer> staleDataMap;
    private final Map<Integer, Integer> tombstoneLastAssociateDataFileMap;
    // files
    private DBFile currentDBFile;

    public void markDataAsStale(byte[] key, IndexMetadata existedMetadata) {
        int existedRecordSize = Record.HEADER_SIZE + key.length + existedMetadata.getValueSize();
        CompactionUtils.recordStaleData(staleDataMap, existedMetadata.getFileId(), existedRecordSize);
    }

    private IndexMetadata writeToCurrentDBFile(Record entry) throws IOException {
        rolloverCurrentDBFile(entry); // TODO impl
        return currentDBFile.writeRecord(entry);
    }
}
