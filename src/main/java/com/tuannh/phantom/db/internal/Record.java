package com.tuannh.phantom.db.internal;

import com.tuannh.phantom.commons.number.NumberUtils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class Record {
    private final byte[] key;
    private final byte[] value;

    @NoArgsConstructor
    @Getter
    @Setter
    public static class Header {
        private static final int HEADER_SIZE = 1 + 1 + 4 + 8 + 4; // version(1), key_size(1), value_size(4), sequence_number(8), checksum(4)
        private static final int HEADER_SIZE_WITHOUT_CHECKSUM = 1 + 1 + 4 + 8;
        private static final int VERSION_OFFSET = 0;
        private static final int KEY_SIZE_OFFSET = 1;
        private static final int VALUE_SIZE_OFFSET = 1 + 1;
        private static final int SEQUENCE_NUMBER_OFFSET = 1 + 1 + 4;
        private static final int CHECKSUM_OFFSET = 1 + 1 + 4 + 8;

        private byte version;
        private byte keySize;
        private int valueSize;
        private long sequenceNumber;
        @Getter(AccessLevel.NONE)
        @Setter(AccessLevel.NONE)
        private long checksum;

        public Header(byte version, byte keySize, int valueSize, long sequenceNumber, long checksum) throws IOException {
            this.version = version;
            this.keySize = keySize;
            this.valueSize = valueSize;
            this.sequenceNumber = sequenceNumber;
            ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
            buffer.put(VERSION_OFFSET, version);
            buffer.put(KEY_SIZE_OFFSET, keySize);
            buffer.putInt(VALUE_SIZE_OFFSET, valueSize);
            buffer.putLong(SEQUENCE_NUMBER_OFFSET, sequenceNumber);
            long calculatedChecksum = checksum(buffer);
            if (checksum != calculatedChecksum) {
                throw new IOException("checksum failed. record header corrupted");
            }
            this.checksum = checksum;
        }

        private long checksum(ByteBuffer buffer) {
            CRC32 crc32 = new CRC32();
            crc32.update(buffer.array(), 0, HEADER_SIZE_WITHOUT_CHECKSUM);
            return crc32.getValue();
        }

        public ByteBuffer serialize() {
            ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
            buffer.put(VERSION_OFFSET, version);
            buffer.put(KEY_SIZE_OFFSET, keySize);
            buffer.putInt(VALUE_SIZE_OFFSET, valueSize);
            buffer.putLong(SEQUENCE_NUMBER_OFFSET, sequenceNumber);
            checksum = checksum(buffer);
            buffer.putInt(CHECKSUM_OFFSET, NumberUtils.fromUInt32(checksum));
            return buffer;
        }

        public static Header deserialize(ByteBuffer buffer) throws IOException {
            byte version = buffer.get(VERSION_OFFSET);
            byte keySize = buffer.get(KEY_SIZE_OFFSET);
            int valueSize = buffer.getInt(VALUE_SIZE_OFFSET);
            long sequenceNumber = buffer.getLong(SEQUENCE_NUMBER_OFFSET);
            long checksum = NumberUtils.toUInt32(buffer.getInt(CHECKSUM_OFFSET));
            return new Header(version, keySize, valueSize, sequenceNumber, checksum);
        }
    }
}
