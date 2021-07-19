package com.tuannh.phantom.offheap.hashtable.segment;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class Address {
    private final short chunkIndex;
    private final int chunkOffset;
}
