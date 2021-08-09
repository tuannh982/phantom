package io.github.tuannh982.phantom.commons.assertion;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Assertions {
    public static void inRangeChecks(int lBound, int hBound, int val) {
        if (val < lBound || val >= hBound) {
            throw new ArrayIndexOutOfBoundsException();
        }
    }

    public static <T> void equalChecks(T l, T r) {
        if (l != r) {
            throw new IllegalStateException();
        }
    }
}
