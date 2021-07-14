package com.tuannh.phantom.db.internal.file;

import com.tuannh.phantom.commons.number.NumberUtils;
import com.tuannh.phantom.db.internal.Versions;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

@Getter
public class TombstoneFileEntry {
    public static final int HEADER_SIZE = 1 + 1 + 8 + 4; // version(1), key_size(1), sequence_number(8), checksum(4)
    public static final int HEADER_SIZE_WITHOUT_CHECKSUM = 1 + 1 + 8;
    public static final int VERSION_OFFSET = 0;
    public static final int KEY_SIZE_OFFSET = 1;
    public static final int SEQUENCE_NUMBER_OFFSET = 1 + 1;
    public static final int CHECKSUM_OFFSET = 1 + 1 + 8;

    private final Header header;
    private final byte[] key;

    public TombstoneFileEntry(byte[] key) {
        this.key = key;
        this.header = new Header(Versions.TOMBSTONE_FILE_VERSION, (byte) key.length, -1, 0);
    }

    public TombstoneFileEntry(byte[] key, Header header) {
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

    public int serializedSize() {
        return HEADER_SIZE + key.length;
    }

    public static TombstoneFileEntry deserialize(ByteBuffer buffer, Header header) {
        buffer.flip();
        byte[] key = new byte[header.keySize];
        buffer.get(key);
        return new TombstoneFileEntry(key, header);
    }

    @AllArgsConstructor
    @Getter
    @Setter
    public static class Header {
        private byte version;
        private byte keySize;
        private long sequenceNumber;
        private long checksum;

        public ByteBuffer serialize() {
            ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
            buffer.put(VERSION_OFFSET, version);
            buffer.put(KEY_SIZE_OFFSET, keySize);
            buffer.putLong(SEQUENCE_NUMBER_OFFSET, sequenceNumber);
            buffer.putInt(CHECKSUM_OFFSET, NumberUtils.fromUInt32(checksum));
            return buffer;
        }

        public static Header deserialize(ByteBuffer buffer) {
            byte version = buffer.get(VERSION_OFFSET);
            byte keySize = buffer.get(KEY_SIZE_OFFSET);
            long sequenceNumber = buffer.getLong(SEQUENCE_NUMBER_OFFSET);
            long checksum = NumberUtils.toUInt32(buffer.getInt(CHECKSUM_OFFSET));
            return new Header(version, keySize, sequenceNumber, checksum);
        }
    }
}
