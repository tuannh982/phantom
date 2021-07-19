package com.github.tuannh982.phantom.db.index.offheap;

import com.github.tuannh982.phantom.offheap.hashtable.ValueSerializer;
import com.github.tuannh982.phantom.db.index.IndexMetadata;

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
