package com.tuannh.phantom.db.internal.file;

import com.tuannh.phantom.db.internal.DBDirectory;
import com.tuannh.phantom.db.internal.PhantomDBOptions;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

@Slf4j
public class IndexFile implements Closeable {
    private static final String INDEX_FILE_EXTENSION = ".index";
    private static final String INDEX_REPAIR_FILE_EXTENSION = ".index.repair";

    private final int fileId;
    private final DBDirectory dbDirectory;
    private final PhantomDBOptions dbOptions;
    //
    private final File file;
    private final FileChannel channel;
    //
    private long unflushed = 0;

    private IndexFile(int fileId, DBDirectory dbDirectory, PhantomDBOptions dbOptions, File file, FileChannel channel) {
        this.fileId = fileId;
        this.dbDirectory = dbDirectory;
        this.dbOptions = dbOptions;
        this.file = file;
        this.channel = channel;
    }

    public File file() {
        return file;
    }

    public Path path() {
        return file.toPath();
    }

    @SuppressWarnings("java:S2095")
    public static IndexFile create(int fileId, DBDirectory dbDirectory, PhantomDBOptions dbOptions) throws IOException {
        File file = dbDirectory.path().resolve(fileId + INDEX_FILE_EXTENSION).toFile();
        boolean b = file.createNewFile();
        if (!b) {
            throw new IOException(file.getName() + " already existed");
        }
        FileChannel channel = new RandomAccessFile(file, "rw").getChannel();
        return new IndexFile(fileId, dbDirectory, dbOptions, file, channel);
    }

    @SuppressWarnings("java:S2095")
    public static IndexFile open(int fileId, DBDirectory dbDirectory, PhantomDBOptions dbOptions) throws IOException {
        Path path = dbDirectory.path().resolve(fileId + INDEX_FILE_EXTENSION);
        if (!Files.exists(path)) {
            throw new IOException(path.toString() + " did not exists");
        }
        File file = path.toFile();
        FileChannel channel = new RandomAccessFile(file, "rw").getChannel();
        return new IndexFile(fileId, dbDirectory, dbOptions, file, channel);
    }

    @SuppressWarnings({"java:S2095", "java:S4042", "java:S899", "ResultOfMethodCallIgnored"})
    public static IndexFile createRepairFile(int fileId, DBDirectory dbDirectory, PhantomDBOptions dbOptions) throws IOException {
        File file = dbDirectory.path().resolve(fileId + INDEX_REPAIR_FILE_EXTENSION).toFile();
        while (!file.createNewFile()) {
            log.error("file already exists, try deleting...");
            file.delete();
        }
        FileChannel channel = new RandomAccessFile(file, "rw").getChannel();
        return new IndexFile(fileId, dbDirectory, dbOptions, file, channel);
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
    }

    @Override
    public void close() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
    }

    public void write(IndexFileEntry entry) throws IOException {
        ByteBuffer[] buffers = entry.serialize();
        long toBeWritten = 0;
        for (ByteBuffer buffer : buffers) {
            toBeWritten += buffer.remaining();
        }
        long written = 0;
        while (written < toBeWritten) {
            written += channel.write(buffers);
        }
        unflushed += written;
        if (unflushed > dbOptions.getDataFlushThreshold()) {
            flush();
            unflushed = 0;
        }
    }

    public Iterator<IndexFileEntry> iterator() throws IOException {
        return new IndexFileIterator();
    }

    private class IndexFileIterator implements Iterator<IndexFileEntry> {
        private final long channelSize;
        private long offset = 0;

        public IndexFileIterator() throws IOException {
            channelSize = channel.size();
        }

        @Override
        public boolean hasNext() {
            return offset < channelSize;
        }

        @Override
        public IndexFileEntry next() {
            if (hasNext()) {
                try {
                    ByteBuffer headerBuffer = ByteBuffer.allocate(IndexFileEntry.HEADER_SIZE);
                    offset += channel.read(headerBuffer);
                    IndexFileEntry.Header header = IndexFileEntry.Header.deserialize(headerBuffer);
                    ByteBuffer dataBuffer = ByteBuffer.allocate(header.getKeySize());
                    offset += channel.read(dataBuffer);
                    IndexFileEntry entry = IndexFileEntry.deserialize(dataBuffer, header);
                    if (!entry.verifyChecksum()) {
                        throw new IOException("checksum failed");
                    }
                    return entry;
                } catch (IOException e) {
                    log.error("index file corrupted ", e);
                    offset = channelSize;
                }
            }
            return null;
        }
    }
}
