package com.tuannh.phantom.db.internal;

public class DBMetadata {
    /**
     * checksum(4), version(1), is_open(1), io_error(1), max_file_size(4)
     */
    private static final int METADATA_SIZE = 4 + 1 + 1 + 1 + 4;

    private long checksum;
    private byte version;
    private boolean open;
    private boolean ioError;
    private int maxFileSize;

    // TODO
}
