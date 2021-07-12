package com.tuannh.phantom.db.internal;

import com.tuannh.phantom.commons.number.NumberUtils;
import lombok.*;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

@Getter
public class Record {
    /**
     * Header
     * key
     * value
     */
    private static final int HEADER_SIZE = 1 + 1 + 4 + 8 + 4; // version(1), key_size(1), value_size(4), sequence_number(8), checksum(4)
    private static final int HEADER_SIZE_WITHOUT_CHECKSUM = 1 + 1 + 4 + 8;
    private static final int VERSION_OFFSET = 0;
    private static final int KEY_SIZE_OFFSET = 1;
    private static final int VALUE_SIZE_OFFSET = 1 + 1;
    private static final int SEQUENCE_NUMBER_OFFSET = 1 + 1 + 4;
    private static final int CHECKSUM_OFFSET = 1 + 1 + 4 + 8;

    private final Header header;
    private final byte[] key;
    private final byte[] value;
    private final int recordSize;

    public Record(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
        this.header = new Header(Versions.DATA_FILE_VERSION, (byte) key.length, value.length, -1, 0);
        recordSize = key.length + value.length + HEADER_SIZE;
    }

    public Record(byte[] key, byte[] value, Header header) {
        this.key = key;
        this.value = value;
        this.header = header;
        recordSize = key.length + value.length + HEADER_SIZE;
    }

    private long checksum(ByteBuffer buffer) {
        CRC32 crc32 = new CRC32();
        crc32.update(buffer.array(), 0, HEADER_SIZE_WITHOUT_CHECKSUM);
        crc32.update(key);
        crc32.update(value);
        return crc32.getValue();
    }

    public boolean verifyChecksum() {
        long checksum = checksum(header.serialize());
        return checksum == header.checksum;
    }

    public ByteBuffer[] serialize() {
        header.checksum = checksum(header.serialize()); // just for checksum, overwrite checksum later
        ByteBuffer headerBuffer = header.serialize();
        return new ByteBuffer[] {headerBuffer, ByteBuffer.wrap(key), ByteBuffer.wrap(value)};
    }

    public static Record deserialize(ByteBuffer buffer, Header header) {
        buffer.flip();
        byte[] key = new byte[header.keySize];
        byte[] value = new byte[header.valueSize];
        buffer.get(key);
        buffer.get(value);
        return new Record(key, value, header);
    }

    @AllArgsConstructor
    @Getter
    @Setter
    public static class Header {
        private byte version;
        private byte keySize;
        private int valueSize;
        private long sequenceNumber;
        private long checksum;

        public ByteBuffer serialize() {
            ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
            buffer.put(VERSION_OFFSET, version);
            buffer.put(KEY_SIZE_OFFSET, keySize);
            buffer.putInt(VALUE_SIZE_OFFSET, valueSize);
            buffer.putLong(SEQUENCE_NUMBER_OFFSET, sequenceNumber);
            buffer.putInt(CHECKSUM_OFFSET, NumberUtils.fromUInt32(checksum));
            return buffer;
        }

        public static Header deserialize(ByteBuffer buffer) {
            byte version = buffer.get(VERSION_OFFSET);
            byte keySize = buffer.get(KEY_SIZE_OFFSET);
            int valueSize = buffer.getInt(VALUE_SIZE_OFFSET);
            long sequenceNumber = buffer.getLong(SEQUENCE_NUMBER_OFFSET);
            long checksum = NumberUtils.toUInt32(buffer.getInt(CHECKSUM_OFFSET));
            return new Header(version, keySize, valueSize, sequenceNumber, checksum);
        }
    }
}
