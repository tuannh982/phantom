package com.tuannh.phantom.db.internal;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Versions {
    public static final byte DATA_FILE_VERSION = (byte) 0xF1;
    public static final byte INDEX_FILE_VERSION = (byte) 0x34;
    public static final byte TOMBSTONE_FILE_VERSION = (byte) 0x75;
    public static final byte METADATA_FILE_VERSION = (byte) 0x1d;
}
