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
    private File file;
    private FileChannel channel;
    //
    private long unflushed = 0;

    public IndexFile(int fileId, DBDirectory dbDirectory, PhantomDBOptions dbOptions) {
        this.fileId = fileId;
        this.dbDirectory = dbDirectory;
        this.dbOptions = dbOptions;
    }

    public File file() {
        return file;
    }

    public Path path() {
        return file.toPath();
    }

    @SuppressWarnings("java:S2095")
    public void create() throws IOException {
        file = dbDirectory.path().resolve(fileId + INDEX_FILE_EXTENSION).toFile();
        boolean b = file.createNewFile();
        if (!b) {
            throw new IOException(file.getName() + " already existed");
        }
        channel = new RandomAccessFile(file, "rw").getChannel();
    }

    @SuppressWarnings("java:S2095")
    public void open() throws IOException {
        Path path = dbDirectory.path().resolve(fileId + INDEX_FILE_EXTENSION);
        if (!Files.exists(path)) {
            throw new IOException(path.toString() + " did not exists");
        }
        channel = new RandomAccessFile(file, "rw").getChannel();
    }

    @SuppressWarnings({"java:S4042", "java:S899", "ResultOfMethodCallIgnored"})
    public void delete() throws IOException {
        file = dbDirectory.path().resolve(fileId + INDEX_FILE_EXTENSION).toFile();
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
        file.delete();
    }

    @SuppressWarnings({"java:S2095", "java:S4042", "java:S899", "ResultOfMethodCallIgnored"})
    public void createRepairFile() throws IOException {
        file = dbDirectory.path().resolve(fileId + INDEX_REPAIR_FILE_EXTENSION).toFile();
        while (!file.createNewFile()) {
            file.delete();
        }
        channel = new RandomAccessFile(file, "rw").getChannel();
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

    public IndexFileIterator iterator() throws IOException {
        return new IndexFileIterator(channel);
    }

    public static class IndexFileIterator implements Iterator<IndexFileEntry> {
        private final FileChannel channel;
        private final long channelSize;
        private long offset = 0;

        public IndexFileIterator(FileChannel channel) throws IOException {
            this.channel = channel;
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
                    return IndexFileEntry.deserialize(dataBuffer, header);
                } catch (IOException e) {
                    log.error("index file corrupted ", e);
                    offset = channelSize;
                }
            }
            return null;
        }
    }
}
