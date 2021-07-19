package com.tuannh.phantom.offheap.hashtable.segment;

import com.tuannh.phantom.commons.unsafe.UnsafeWrapper;
import lombok.Getter;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

@Getter
public class Chunk implements Closeable {
    // serialized_data = address(x), key_length(1), key(max_key_size), value(fixed_value_size)
    public static final int KEY_LENGTH_OFFSET = Address.SERIALIZED_SIZE;
    public static final int DATA_OFFSET = KEY_LENGTH_OFFSET + 1;

    // primary
    private final int maxKeySize;
    private final int fixedValueSize;
    private final int memoryChunkSize;
    private final long address;
    private final int entrySize;
    //
    private int writeOffset;

    private Chunk(int maxKeySize, int fixedValueSize, int memoryChunkSize, long address) {
        this.maxKeySize = maxKeySize;
        this.fixedValueSize = fixedValueSize;
        this.memoryChunkSize = memoryChunkSize;
        this.address = address;
        this.writeOffset = 0;
        this.entrySize = DATA_OFFSET + maxKeySize + fixedValueSize;
    }

    public static Chunk allocate(int maxKeySize, int fixedValueSize, int memoryChunkSize) {
        long address = UnsafeWrapper.malloc(memoryChunkSize);
        return new Chunk(maxKeySize, fixedValueSize, memoryChunkSize, address);
    }

    public Address getNextAddress(int offset) {
        return Address.deserialize(address + offset);
    }

    public void setNextAddress(int offset, Address nextAddress) {
        nextAddress.serialize(address + offset);
    }

    public ByteBuffer readKey(int offset) {
        int keyLength = UnsafeWrapper.getByte(address, (long)offset + KEY_LENGTH_OFFSET);
        return UnsafeWrapper.directBuffer(address, (long)offset + DATA_OFFSET, keyLength, true);
    }

    public void setKey(int offset, byte[] key) {
        UnsafeWrapper.putByte(address, (long)offset + KEY_LENGTH_OFFSET, (byte) key.length);
        UnsafeWrapper.memcpy(key, 0, address, (long)offset + DATA_OFFSET, key.length);
    }

    public ByteBuffer readValue(int offset) {
        int valueOffset = offset + DATA_OFFSET + maxKeySize;
        return UnsafeWrapper.directBuffer(address, valueOffset, fixedValueSize, true);
    }

    public void setValue(int offset, byte[] value) {
        int valueOffset = offset + DATA_OFFSET + maxKeySize;
        UnsafeWrapper.memcpy(value, 0, address, valueOffset, value.length);
    }

    public void writeEntry(byte[] key, byte[] value, Address nextAddress) {
        setNextAddress(writeOffset, nextAddress);
        setKey(writeOffset, key);
        setValue(writeOffset, value);
        writeOffset += entrySize;
    }

    public int remaining() {
        return memoryChunkSize - writeOffset;
    }

    @Override
    public void close() throws IOException {
        UnsafeWrapper.free(address);
    }
}
