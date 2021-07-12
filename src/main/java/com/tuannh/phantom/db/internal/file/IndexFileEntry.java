package com.tuannh.phantom.db.internal.file;

import com.tuannh.phantom.commons.number.NumberUtils;
import com.tuannh.phantom.db.internal.Versions;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

@Getter
public class IndexFileEntry {
    public static final int HEADER_SIZE = 1 + 1 + 4 + 4 + 8 + 4; // version(1), key_size(1), record_offset(4), record_size(4), sequence_number(8), checksum(4)
    public static final int HEADER_SIZE_WITHOUT_CHECKSUM = 1 + 1 + 4 + 4 + 8;
    public static final int VERSION_OFFSET = 0;
    public static final int KEY_SIZE_OFFSET = 1;
    public static final int RECORD_OFFSET_OFFSET = 1 + 1;
    public static final int RECORD_SIZE_OFFSET = 1 + 1 + 4;
    public static final int SEQUENCE_NUMBER_OFFSET = 1 + 1 + 4 + 4;
    public static final int CHECKSUM_OFFSET = 1 + 1 + 4 + 4 + 8;

    private final Header header;
    private final byte[] key;

    public IndexFileEntry(byte[] key, int recordOffset, int recordSize) {
        this.key = key;
        this.header = new Header(Versions.INDEX_FILE_VERSION, (byte) key.length, recordOffset, recordSize, -1, 0);
    }

    public IndexFileEntry(byte[] key, Header header) {
        this.key = key;
        this.header = header;
    }

    private long checksum(ByteBuffer buffer) {
        CRC32 crc32 = new CRC32();
        crc32.update(buffer.array(), 0, HEADER_SIZE_WITHOUT_CHECKSUM);
        crc32.update(key);
        return crc32.getValue();
    }

    public boolean verifyChecksum() {
        long checksum = checksum(header.serialize());
        return checksum == header.checksum;
    }

    public ByteBuffer[] serialize() {
        header.checksum = checksum(header.serialize()); // just for checksum, overwrite checksum later
        ByteBuffer headerBuffer = header.serialize();
        return new ByteBuffer[] {headerBuffer, ByteBuffer.wrap(key)};
    }

    public static IndexFileEntry deserialize(ByteBuffer buffer, Header header) {
        buffer.flip();
        byte[] key = new byte[header.keySize];
        buffer.get(key);
        return new IndexFileEntry(key, header);
    }

    @AllArgsConstructor
    @Getter
    @Setter
    public static class Header {
        private byte version;
        private byte keySize;
        private int recordOffset;
        private int recordSize;
        private long sequenceNumber;
        private long checksum;

        public ByteBuffer serialize() {
            ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
            buffer.put(VERSION_OFFSET, version);
            buffer.put(KEY_SIZE_OFFSET, keySize);
            buffer.putInt(RECORD_OFFSET_OFFSET, recordOffset);
            buffer.putInt(RECORD_SIZE_OFFSET, recordSize);
            buffer.putLong(SEQUENCE_NUMBER_OFFSET, sequenceNumber);
            buffer.putInt(CHECKSUM_OFFSET, NumberUtils.fromUInt32(checksum));
            return buffer;
        }

        public static Header deserialize(ByteBuffer buffer) {
            byte version = buffer.get(VERSION_OFFSET);
            byte keySize = buffer.get(KEY_SIZE_OFFSET);
            int recordOffset = buffer.getInt(RECORD_OFFSET_OFFSET);
            int recordSize = buffer.getInt(RECORD_SIZE_OFFSET);
            long sequenceNumber = buffer.getLong(SEQUENCE_NUMBER_OFFSET);
            long checksum = NumberUtils.toUInt32(buffer.getInt(CHECKSUM_OFFSET));
            return new Header(version, keySize, recordOffset, recordSize, sequenceNumber, checksum);
        }
    }
}
