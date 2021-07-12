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

@NoArgsConstructor
@Getter
@Setter
public class DBMetadata {
    private static final String METADATA_FILENAME = "METADATA";
    private static final int METADATA_SIZE = 1 + 1 + 1 + 4 + 4; // version(1), is_open(1), io_error(1), max_file_size(4), checksum(4)
    private static final int CHECKSUM_OFFSET = 1 + 1 + 1 + 4;
    private static final int METADATA_SIZE_WITHOUT_CHECKSUM = 1 + 1 + 1 + 4;

    private byte version;
    private boolean open;
    private boolean ioError;
    private int maxFileSize;
    //
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private long checksum;

    private DBMetadata(byte version, boolean open, boolean ioError, int maxFileSize, long checksum) throws IOException {
        this.version = version;
        this.open = open;
        this.ioError = ioError;
        this.maxFileSize = maxFileSize;
        ByteBuffer buffer = ByteBuffer.allocate(METADATA_SIZE);
        buffer.put(version);
        buffer.put((byte) (open ? 0x01 : 0x00));
        buffer.put((byte) (ioError ? 0x01 : 0x00));
        buffer.putInt(maxFileSize);
        CRC32 crc32 = new CRC32();
        crc32.update(buffer.array(), 0, METADATA_SIZE_WITHOUT_CHECKSUM);
        if (checksum != crc32.getValue()) {
            throw new IOException("checksum failed. metadata corrupted");
        }
        this.checksum = checksum;
    }

    public static DBMetadata load(DBDirectory dbDirectory) throws IOException {
        Path path = dbDirectory.path().resolve(METADATA_FILENAME);
        if (Files.exists(path)) {
            try (SeekableByteChannel channel = Files.newByteChannel(path)) {
                ByteBuffer buffer = ByteBuffer.allocate(METADATA_SIZE);
                channel.read(buffer);
                buffer.flip();
                byte version = buffer.get();
                boolean open = buffer.get() != 0;
                boolean ioError = buffer.get() != 0;
                int maxFileSize = buffer.getInt();
                long checksum = NumberUtils.toUInt32(buffer.getInt());
                return new DBMetadata(version, open, ioError, maxFileSize, checksum);
            }
        } else {
            throw new IOException("metadata file not exists");
        }
    }

    private ByteBuffer createBufferAndUpdateChecksum() {
        ByteBuffer buffer = ByteBuffer.allocate(METADATA_SIZE);
        buffer.put(version);
        buffer.put((byte) (open ? 0x01 : 0x00));
        buffer.put((byte) (ioError ? 0x01 : 0x00));
        buffer.putInt(maxFileSize);
        CRC32 crc32 = new CRC32();
        crc32.update(buffer.array(), 0, METADATA_SIZE_WITHOUT_CHECKSUM);
        checksum = crc32.getValue();
        buffer.putInt(NumberUtils.fromUInt32(checksum));
        buffer.flip();
        return buffer;
    }

    public void save(DBDirectory dbDirectory) throws IOException {
        String tempFile = METADATA_FILENAME + ".tmp";
        Path tempPath = dbDirectory.path().resolve(tempFile);
        Files.deleteIfExists(tempPath); // delete previous temp file
        try (FileChannel channel = FileChannel.open(
                tempPath,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.SYNC)
        ) {
            ByteBuffer buffer = createBufferAndUpdateChecksum();
            channel.write(buffer);
            Files.move(tempPath, dbDirectory.path().resolve(METADATA_FILENAME), REPLACE_EXISTING, ATOMIC_MOVE);
            dbDirectory.sync();
        }
    }
}
