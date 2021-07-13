package com.tuannh.phantom.db.internal;

import com.tuannh.phantom.commons.number.NumberUtils;
import lombok.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

@Getter
@Setter
public class DBMetadata {
    private static final String METADATA_FILENAME = "METADATA";
    private static final int METADATA_SIZE = 1 + 1 + 1 + 4 + 4; // version(1), is_open(1), io_error(1), max_file_size(4), checksum(4)
    private static final int METADATA_SIZE_WITHOUT_CHECKSUM = 1 + 1 + 1 + 4;
    private static final int VERSION_OFFSET = 0;
    private static final int OPEN_OFFSET = 1;
    private static final int IO_ERROR_OFFSET = 1 + 1;
    private static final int MAX_FILE_SIZE_OFFSET = 1 + 1 + 1;
    private static final int CHECKSUM_OFFSET = 1 + 1 + 1 + 4;

    private byte version;
    private boolean open;
    private boolean ioError;
    private int maxFileSize;
    //
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private long checksum;

    public DBMetadata() {
        this.version = Versions.METADATA_FILE_VERSION;
        this.open = false;
        this.ioError = false;
        this.maxFileSize = 0;
        this.checksum = 0;
    }

    private DBMetadata(byte version, boolean open, boolean ioError, int maxFileSize, long checksum) throws IOException {
        this.version = version;
        this.open = open;
        this.ioError = ioError;
        this.maxFileSize = maxFileSize;
        ByteBuffer buffer = ByteBuffer.allocate(METADATA_SIZE);
        buffer.put(VERSION_OFFSET, version);
        buffer.put(OPEN_OFFSET, (byte) (open ? 0x01 : 0x00));
        buffer.put(IO_ERROR_OFFSET, (byte) (ioError ? 0x01 : 0x00));
        buffer.putInt(MAX_FILE_SIZE_OFFSET, maxFileSize);
        long calculatedChecksum = checksum(buffer);
        if (checksum != calculatedChecksum) {
            throw new IOException("checksum failed. metadata corrupted");
        }
        this.checksum = checksum;
    }

    private long checksum(ByteBuffer buffer) {
        CRC32 crc32 = new CRC32();
        crc32.update(buffer.array(), 0, METADATA_SIZE_WITHOUT_CHECKSUM);
        return crc32.getValue();
    }

    public ByteBuffer serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(METADATA_SIZE);
        buffer.put(VERSION_OFFSET, version);
        buffer.put(OPEN_OFFSET, (byte) (open ? 0x01 : 0x00));
        buffer.put(IO_ERROR_OFFSET, (byte) (ioError ? 0x01 : 0x00));
        buffer.putInt(MAX_FILE_SIZE_OFFSET, maxFileSize);
        checksum = checksum(buffer);
        buffer.putInt(CHECKSUM_OFFSET, NumberUtils.fromUInt32(checksum));
        return buffer;
    }

    public static DBMetadata deserialize(ByteBuffer buffer) throws IOException {
        byte version = buffer.get(VERSION_OFFSET);
        boolean open = buffer.get(OPEN_OFFSET) != 0;
        boolean ioError = buffer.get(IO_ERROR_OFFSET) != 0;
        int maxFileSize = buffer.getInt(MAX_FILE_SIZE_OFFSET);
        long checksum = NumberUtils.toUInt32(buffer.getInt(CHECKSUM_OFFSET));
        return new DBMetadata(version, open, ioError, maxFileSize, checksum);
    }

    public static DBMetadata load(DBDirectory dbDirectory) throws IOException {
        Path path = dbDirectory.path().resolve(METADATA_FILENAME);
        if (Files.exists(path)) {
            try (SeekableByteChannel channel = Files.newByteChannel(path)) {
                ByteBuffer buffer = ByteBuffer.allocate(METADATA_SIZE);
                channel.read(buffer);
                buffer.flip();
                return deserialize(buffer);
            }
        } else {
            throw new IOException("metadata file not exists");
        }
    }

    public static DBMetadata load(DBDirectory dbDirectory, DBMetadata defaultDbMetadata) throws IOException {
        Path path = dbDirectory.path().resolve(METADATA_FILENAME);
        if (Files.exists(path)) {
            try (SeekableByteChannel channel = Files.newByteChannel(path)) {
                ByteBuffer buffer = ByteBuffer.allocate(METADATA_SIZE);
                channel.read(buffer);
                buffer.flip();
                return deserialize(buffer);
            }
        } else {
            return defaultDbMetadata;
        }
    }

    public void save(DBDirectory dbDirectory) throws IOException {
        String tempFile = METADATA_FILENAME + ".tmp";
        Path tempPath = dbDirectory.path().resolve(tempFile);
        Files.deleteIfExists(tempPath); // delete previous temp file
        try (FileChannel channel = FileChannel.open(
                tempPath,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.SYNC)
        ) {
            ByteBuffer buffer = serialize();
            channel.write(buffer);
            Files.move(tempPath, dbDirectory.path().resolve(METADATA_FILENAME), REPLACE_EXISTING, ATOMIC_MOVE);
            dbDirectory.sync();
        }
    }
}
