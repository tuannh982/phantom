package com.tuannh.phantom.db.internal.file;

import com.tuannh.phantom.commons.io.FileUtils;
import com.tuannh.phantom.commons.number.NumberUtils;
import com.tuannh.phantom.db.index.IndexMetadata;
import com.tuannh.phantom.db.internal.DBDirectory;
import com.tuannh.phantom.db.internal.PhantomDBOptions;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

@Slf4j
@Getter
public class DBFile implements Closeable {
    private static final String DATA_FILE_EXTENSION = ".data";
    private static final String COMPACT_FILE_EXTENSION = ".datac";
    private static final String REPAIR_FILE_EXTENSION = ".repair";

    private final int fileId;
    private final DBDirectory dbDirectory;
    private final PhantomDBOptions dbOptions;
    private final boolean compacted;
    //
    private final File file;
    private final IndexFile indexFile;
    private final FileChannel channel;
    //
    private int unflushed = 0;
    private int writeOffset = 0;

    private DBFile(int fileId, DBDirectory dbDirectory, PhantomDBOptions dbOptions, boolean compacted, File file, IndexFile indexFile, FileChannel channel) throws IOException {
        this.fileId = fileId;
        this.dbDirectory = dbDirectory;
        this.dbOptions = dbOptions;
        this.compacted = compacted;
        this.file = file;
        this.indexFile = indexFile;
        this.channel = channel;
        this.writeOffset = NumberUtils.checkedCast(channel.size());
    }

    public File file() {
        return file;
    }

    public Path path() {
        return file.toPath();
    }

    @SuppressWarnings("java:S2095")
    public static DBFile create(int fileId, DBDirectory dbDirectory, PhantomDBOptions dbOptions, boolean compacted) throws IOException {
        String fileExtension = compacted ? COMPACT_FILE_EXTENSION : DATA_FILE_EXTENSION;
        File file = dbDirectory.path().resolve(fileId + fileExtension).toFile();
        while (!file.createNewFile()) {
            fileId++;
            file = dbDirectory.path().resolve(fileId + fileExtension).toFile();
        }
        FileChannel channel = new RandomAccessFile(file, "rw").getChannel();
        IndexFile indexFile = IndexFile.create(fileId, dbDirectory, dbOptions);
        return new DBFile(fileId, dbDirectory, dbOptions, compacted, file, indexFile, channel);
    }

    @SuppressWarnings("java:S2095")
    public static DBFile openForRead(int fileId, DBDirectory dbDirectory, PhantomDBOptions dbOptions, boolean compacted) throws IOException {
        String fileExtension = compacted ? COMPACT_FILE_EXTENSION : DATA_FILE_EXTENSION;
        File file = dbDirectory.path().resolve(fileId + fileExtension).toFile();
        FileChannel channel = new RandomAccessFile(file, "r").getChannel();
        IndexFile indexFile = IndexFile.open(fileId, dbDirectory, dbOptions);
        return new DBFile(fileId, dbDirectory, dbOptions, compacted, file, indexFile, channel);
    }

    public DBFile repairAndOpenForRead() throws IOException {
        DBFile repairFile = createRepairFile(fileId, dbDirectory, dbOptions, compacted);
        Iterator<Record> iterator = iterator();
        while (iterator.hasNext()) {
            Record entry = iterator.next();
            if (entry == null) { // no need to verify checksum
                break;
            }
            repairFile.writeRecord(entry);
        }
        repairFile.close();
        close();
        Files.move(repairFile.indexFile.path(), indexFile.path(), REPLACE_EXISTING, ATOMIC_MOVE);
        Files.move(repairFile.path(), path(), REPLACE_EXISTING, ATOMIC_MOVE);
        dbDirectory.sync();
        return openForRead(fileId, dbDirectory, dbOptions, compacted);
    }

    @SuppressWarnings({"java:S2095", "java:S4042", "java:S899", "ResultOfMethodCallIgnored"})
    public static DBFile createRepairFile(int fileId, DBDirectory dbDirectory, PhantomDBOptions dbOptions, boolean compacted) throws IOException {
        String fileExtension = compacted ? COMPACT_FILE_EXTENSION : DATA_FILE_EXTENSION;
        File file = dbDirectory.path().resolve(fileId + fileExtension + REPAIR_FILE_EXTENSION).toFile();
        while (!file.createNewFile()) {
            log.error("file already exists, try deleting...");
            file.delete();
        }
        FileChannel channel = new RandomAccessFile(file, "rw").getChannel();
        IndexFile indexFile = IndexFile.createRepairFile(fileId, dbDirectory, dbOptions);
        return new DBFile(fileId, dbDirectory, dbOptions, compacted, file, indexFile, channel);
    }

    public void flushToDisk() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.force(true);
        }
    }

    public void flush() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.force(false); // no need to write metadata
        }
    }

    @SuppressWarnings({"java:S4042", "java:S899"})
    public void delete() {
        if (file != null) {
            file.delete();
        }
        if (indexFile != null) {
            indexFile.delete();
        }
    }

    @Override
    public void close() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.force(true);
            channel.close();
        }
        if (indexFile != null) {
            indexFile.close();
        }
    }

    public IndexMetadata writeRecord(Record entry) throws IOException {
        int recordOffset = writeOffset;
        write(entry);
        IndexFileEntry indexFileEntry = new IndexFileEntry(
                entry.getKey(),
                recordOffset,
                entry.getRecordSize()
        );
        indexFileEntry.getHeader().setSequenceNumber(entry.getHeader().getSequenceNumber());
        indexFile.write(indexFileEntry);
        int valueOffset = entry.valueOffset(recordOffset);
        return new IndexMetadata(
                fileId,
                valueOffset,
                entry.getHeader().getValueSize(),
                entry.getHeader().getSequenceNumber()
        );
    }

    public byte[] read(int offset, int len) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(len);
        FileUtils.read(channel, offset, buffer);
        buffer.flip();
        return buffer.array();
    }

    @SuppressWarnings("java:S1854")
    public Record read(int offset) throws IOException {
        int currentOffset = offset;
        ByteBuffer headerBuffer = ByteBuffer.allocate(Record.HEADER_SIZE);
        currentOffset += FileUtils.read(channel, currentOffset, headerBuffer);
        Record.Header header = Record.Header.deserialize(headerBuffer);
        ByteBuffer dataBuffer = ByteBuffer.allocate(header.getKeySize() + header.getValueSize());
        currentOffset += FileUtils.read(channel, currentOffset, dataBuffer);
        Record entry = Record.deserialize(dataBuffer, header);
        if (!entry.verifyChecksum()) {
            throw new IOException("checksum failed");
        }
        return entry;
    }

    public void write(Record entry) throws IOException {
        ByteBuffer[] buffers = entry.serialize();
        long toBeWritten = 0;
        for (ByteBuffer buffer : buffers) {
            toBeWritten += buffer.remaining();
        }
        long written = 0;
        while (written < toBeWritten) {
            written += channel.write(buffers);
        }
        writeOffset += written;
        unflushed += written;
        if (unflushed > dbOptions.getDataFlushThreshold()) {
            flush();
            unflushed = 0;
        }
    }

    public Iterator<Record> iterator() throws IOException {
        return new DBFileIterator();
    }

    private class DBFileIterator implements Iterator<Record> {
        private final FileChannel iterChannel;
        private final long channelSize;
        private long offset = 0;

        public DBFileIterator() throws IOException {
            iterChannel = FileChannel.open(path(), StandardOpenOption.READ);
            channelSize = iterChannel.size();
        }

        @Override
        public boolean hasNext() {
            return offset < channelSize;
        }

        @Override
        public Record next() {
            if (hasNext()) {
                try {
                    ByteBuffer headerBuffer = ByteBuffer.allocate(Record.HEADER_SIZE);
                    offset += FileUtils.read(iterChannel, offset, headerBuffer);
                    Record.Header header = Record.Header.deserialize(headerBuffer);
                    ByteBuffer dataBuffer = ByteBuffer.allocate(header.getKeySize() + header.getValueSize());
                    offset += FileUtils.read(iterChannel, offset, dataBuffer);
                    Record entry = Record.deserialize(dataBuffer, header);
                    if (!entry.verifyChecksum()) {
                        throw new IOException("checksum failed");
                    }
                    return entry;
                } catch (IOException e) {
                    log.error("index file corrupted ", e);
                    offset = channelSize;
                }
            }
            try {
                if (iterChannel != null && iterChannel.isOpen()) {
                    iterChannel.close();
                }
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
            throw new NoSuchElementException();
        }
    }
}
