package com.tuannh.phantom.commons.number;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class NumberUtils {
    public static final int INTEGER_MAX_POWER_OF_TWO = 1 << 30;

    public static long toUInt32(int x) {
        return x & 0xffffffffL;
    }

    public static int fromUInt32(long x) {
        return (int)(x & 0xffffffffL);
    }

    public static byte toUInt8(int x) {
        return (byte)(x & 0xff);
    }

    public static int fromUInt8(byte x) {
        return x & 0xff;
    }

    @SuppressWarnings("all")
    public static int checkedCast(long value) {
        int result = (int)value;
        if ((long)result != value) {
            throw new IllegalArgumentException((new StringBuilder(34)).append("Out of range: ").append(value).toString());
        } else {
            return result;
        }
    }

    public static int roundUpToPowerOf2(int x) {
        if (x >= INTEGER_MAX_POWER_OF_TWO) {
            return INTEGER_MAX_POWER_OF_TWO;
        } else {
            if (x > 1) {
                return Integer.highestOneBit((x - 1) << 1);
            } else {
                return 1;
            }
        }
    }
}
