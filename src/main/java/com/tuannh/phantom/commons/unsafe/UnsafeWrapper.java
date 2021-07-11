package com.tuannh.phantom.commons.unsafe;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

@SuppressWarnings({"java:S1191", "java:S3011"})
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class UnsafeWrapper {
    private static final sun.misc.Unsafe UNSAFE;
    private static final Class<?> DIRECT_BYTE_BUFFER_CLASS;
    private static final Class<?> DIRECT_BYTE_BUFFER_R_CLASS;
    private static final long DIRECT_BYTE_BUFFER_ADDRESS_OFFSET;
    private static final long DIRECT_BYTE_BUFFER_CAPACITY_OFFSET;
    private static final long DIRECT_BYTE_BUFFER_LIMIT_OFFSET;

    static {
        try {
            // get unsafe instance
            Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            UNSAFE = (sun.misc.Unsafe) field.get(null);
            // checks
            if (UNSAFE.addressSize() != 8) {
                throw new IllegalStateException("only support 8 bytes address size");
            }
            // init direct buffer
            ByteBuffer directByteBuffer = ByteBuffer.allocateDirect(0);
            ByteBuffer readOnlyDirectByteBuffer = directByteBuffer.asReadOnlyBuffer();
            DIRECT_BYTE_BUFFER_CLASS = directByteBuffer.getClass();
            DIRECT_BYTE_BUFFER_R_CLASS = readOnlyDirectByteBuffer.getClass();
            DIRECT_BYTE_BUFFER_ADDRESS_OFFSET = UNSAFE.objectFieldOffset(Buffer.class.getDeclaredField("address"));
            DIRECT_BYTE_BUFFER_CAPACITY_OFFSET = UNSAFE.objectFieldOffset(Buffer.class.getDeclaredField("capacity"));
            DIRECT_BYTE_BUFFER_LIMIT_OFFSET = UNSAFE.objectFieldOffset(Buffer.class.getDeclaredField("limit"));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    // byte buffer

    public static ByteBuffer directBuffer(long address, long offset, int len, boolean readOnly) {
        ByteBuffer byteBuffer;
        try {
            if (readOnly) {
                byteBuffer = (ByteBuffer) UNSAFE.allocateInstance(DIRECT_BYTE_BUFFER_R_CLASS);
            } else {
                byteBuffer = (ByteBuffer) UNSAFE.allocateInstance(DIRECT_BYTE_BUFFER_CLASS);
            }
            UNSAFE.putLong(byteBuffer, DIRECT_BYTE_BUFFER_ADDRESS_OFFSET, address + offset);
            UNSAFE.putInt(byteBuffer, DIRECT_BYTE_BUFFER_CAPACITY_OFFSET, len);
            UNSAFE.putInt(byteBuffer, DIRECT_BYTE_BUFFER_LIMIT_OFFSET, len);
            byteBuffer.order(ByteOrder.BIG_ENDIAN);
            return byteBuffer;
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    // memory

    public static long malloc(long size) {
        long address = UNSAFE.allocateMemory(size);
        if (address == 0) {
            throw new OutOfMemoryError("Could not allocate " + size + " bytes");
        }
        return address;
    }

    public static void free(long address) {
        if (address != 0) {
            UNSAFE.freeMemory(address);
        }
    }

    public static void memset(long address, long offset, int len, byte value) {
        UNSAFE.setMemory(address + offset, len, value);
    }

    public static void memcpy(long from, long fromOffset, long to, long toOffset, long len) {
        UNSAFE.copyMemory(null, from + fromOffset, null, to + toOffset, len);
    }

    // special copy
    public static void memcpy(byte[] arr, long offset, long to, long toOffset, long len) {
        UNSAFE.copyMemory(arr, sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET + offset, null, to + toOffset, len);
    }

    // manipulate

    public static void putLong(long address, long offset, long value) {
        UNSAFE.putLong(null, address + offset, value);
    }

    public static long getLong(long address, long offset) {
        return UNSAFE.getLong(null, address + offset);
    }

    public static void putInt(long address, long offset, int value) {
        UNSAFE.putInt(null, address + offset, value);
    }

    public static int getInt(long address, long offset) {
        return UNSAFE.getInt(null, address + offset);
    }

    public static short getShort(long address, long offset) {
        return UNSAFE.getShort(null, address + offset);
    }

    public static void putByte(long address, long offset, byte value) {
        UNSAFE.putByte(null, address + offset, value);
    }

    public static byte getByte(long address, long offset) {
        return UNSAFE.getByte(null, address + offset);
    }
}
