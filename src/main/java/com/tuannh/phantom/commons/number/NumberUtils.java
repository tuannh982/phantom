package com.tuannh.phantom.commons.number;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class NumberUtils {
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
}
