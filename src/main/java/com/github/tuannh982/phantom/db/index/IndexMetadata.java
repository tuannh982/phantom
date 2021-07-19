package com.github.tuannh982.phantom.db.index;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;

@EqualsAndHashCode
@AllArgsConstructor
@Getter
@Setter
public class IndexMetadata {
    public static final int METADATA_SIZE = 4 + 4 + 4 + 8; // file_id(4), value_offset(4), value_size(4), sequence_number(8)
    public static final int FILE_ID_OFFSET = 0;
    public static final int VALUE_OFFSET_OFFSET = 4;
    public static final int VALUE_SIZE_OFFSET = 4 + 4;
    public static final int SEQUENCE_NUMBER_OFFSET = 4 + 4 + 4;

    private int fileId;
    private int valueOffset;
    private int valueSize;
    private long sequenceNumber;

    public ByteBuffer serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(METADATA_SIZE);
        buffer.putInt(FILE_ID_OFFSET, fileId);
        buffer.putInt(VALUE_OFFSET_OFFSET, valueOffset);
        buffer.putInt(VALUE_SIZE_OFFSET, valueSize);
        buffer.putLong(SEQUENCE_NUMBER_OFFSET, sequenceNumber);
        return buffer;
    }

    public static IndexMetadata deserialize(ByteBuffer buffer) {
        int fileId = buffer.getInt(FILE_ID_OFFSET);
        int valueOffset = buffer.getInt(VALUE_OFFSET_OFFSET);
        int valueSize = buffer.getInt(VALUE_SIZE_OFFSET);
        long sequenceNumber = buffer.getLong(SEQUENCE_NUMBER_OFFSET);
        return new IndexMetadata(fileId, valueOffset, valueSize, sequenceNumber);
    }
}
