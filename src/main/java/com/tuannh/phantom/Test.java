package com.tuannh.phantom;

public class Test {
    static int bitNum(long val) {
        int bit = 0;
        for (; val != 0L; bit++) {
            val >>>= 1;
        }
        return bit;
    }

    public static void main(String[] args) {
        System.out.println(bitNum(126));
        long x = (1 << 31);
        System.out.println((int)(x & 0xffffffffL));
    }
}
