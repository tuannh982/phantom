package com.tuannh.phantom.db.index.offheap;

import com.tuannh.phantom.db.index.IndexMetadata;
import com.tuannh.phantom.offheap.hashtable.ValueSerializer;

import java.nio.ByteBuffer;

public class IndexMetadataSerializer implements ValueSerializer<IndexMetadata> {
    @Override
    public ByteBuffer serialize(IndexMetadata value) {
        return value.serialize();
    }

    @Override
    public IndexMetadata deserialize(ByteBuffer buffer) {
        return IndexMetadata.deserialize(buffer);
    }

    @Override
    public int serializedSize() {
        return IndexMetadata.METADATA_SIZE;
    }
}
