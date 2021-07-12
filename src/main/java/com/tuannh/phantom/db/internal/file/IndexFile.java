package com.tuannh.phantom.db.internal.file;

import com.tuannh.phantom.db.internal.DBDirectory;
import com.tuannh.phantom.db.internal.PhantomDBOptions;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public class IndexFile implements Closeable {
    private static final String INDEX_FILE_EXTENSION = ".index";
    private static final String INDEX_REPAIR_FILE_EXTENSION = ".index.repair";

    private final int fileId;
    private final DBDirectory dbDirectory;
    private final PhantomDBOptions dbOptions;
    //
    private File file;
    private FileChannel channel;

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

    public void create() throws IOException {
        file = dbDirectory.path().resolve(fileId + INDEX_FILE_EXTENSION).toFile();
        boolean b = file.createNewFile();
        if (!b) {
            throw new IOException(file.getName() + " already existed");
        }
        channel = new RandomAccessFile(file, "rw").getChannel();
    }


    public void delete() throws IOException {
        file = dbDirectory.path().resolve(fileId + INDEX_FILE_EXTENSION).toFile();
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
        file.delete();
    }

    public void flush() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.force(true);
        }
    }

    @Override
    public void close() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
    }
}
