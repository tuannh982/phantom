package io.github.tuannh982.phantom.offheap.hashtable.segment;

import io.github.tuannh982.phantom.commons.unsafe.UnsafeWrapper;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class Address {
    public static final int SERIALIZED_SIZE = 2 + 4; // chunkIndex(2), chunkOffset(4)
    public static final int CHUNK_INDEX_OFFSET = 0;
    public static final int CHUNK_OFFSET_OFFSET = 2;

    public static final Address NULL_ADDRESS = new Address((short) -1, -1); // 0xffff, 0xffffffff

    private final short chunkIndex;
    private final int chunkOffset;

    public void serialize(long address) {
        UnsafeWrapper.putShort(address, CHUNK_INDEX_OFFSET, chunkIndex);
        UnsafeWrapper.putInt(address, CHUNK_OFFSET_OFFSET, chunkOffset);
    }

    public static Address deserialize(long address) {
        short chunkIndex = UnsafeWrapper.getShort(address, CHUNK_INDEX_OFFSET);
        int chunkOffset = UnsafeWrapper.getInt(address, CHUNK_OFFSET_OFFSET);
        return new Address(chunkIndex, chunkOffset);
    }
}
