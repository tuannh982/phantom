package io.github.tuannh982.phantom.db.index.offheap;

import io.github.tuannh982.phantom.commons.number.NumberUtils;
import io.github.tuannh982.phantom.db.index.IndexMap;
import io.github.tuannh982.phantom.db.index.IndexMetadata;
import io.github.tuannh982.phantom.offheap.hashtable.HashTableOptions;
import io.github.tuannh982.phantom.offheap.hashtable.KeyBuffer;
import io.github.tuannh982.phantom.offheap.hashtable.OffHeapHashTable;
import io.github.tuannh982.phantom.offheap.hashtable.hash.Hasher;
import io.github.tuannh982.phantom.offheap.hashtable.hash.Murmur3;

import java.io.IOException;

public class OffHeapInMemoryIndex implements IndexMap {
    private final OffHeapHashTable<IndexMetadata> hashTable;
    private final Hasher hasher = new Murmur3();

    public OffHeapInMemoryIndex(long estimatedMaxKeyCount, int maxKeySize, int fixedValueSize, int memoryChunkSize) {
        int segmentCount = NumberUtils.roundUpToPowerOf2(2 * Runtime.getRuntime().availableProcessors());
        int entryTableSizePerSegment = 1 << 30;
        long e = estimatedMaxKeyCount / segmentCount;
        if (e < (1 << 30)) {
            entryTableSizePerSegment = (int) e;
        }
        HashTableOptions<IndexMetadata> hashTableOptions = HashTableOptions.<IndexMetadata>builder()
                .maxKeySize(maxKeySize)
                .fixedValueSize(fixedValueSize)
                .memoryChunkSize(memoryChunkSize)
                .segmentCount(segmentCount)
                .entryTableSizePerSegment(entryTableSizePerSegment)
                .valueSerializer(new IndexMetadataSerializer())
                .build();
        this.hashTable = new OffHeapHashTable<>(hashTableOptions);
    }

    @Override
    public IndexMetadata get(byte[] key) {
        return null;
    }

    @Override
    public void put(byte[] key, IndexMetadata metadata) throws IOException {
        hashTable.put(new KeyBuffer(key, hasher), metadata);
    }

    @Override
    public IndexMetadata putIfAbsent(byte[] key, IndexMetadata metadata) throws IOException {
        return hashTable.putIfAbsent(new KeyBuffer(key, hasher), metadata);
    }

    @Override
    public boolean replace(byte[] key, IndexMetadata oldValue, IndexMetadata newValue) throws IOException {
        if (newValue.getSequenceNumber() > oldValue.getSequenceNumber()) {
            return hashTable.replace(new KeyBuffer(key, hasher), oldValue, newValue);
        } else {
            return false;
        }
    }

    @Override
    public void delete(byte[] key) throws IOException {
        hashTable.remove(new KeyBuffer(key, hasher));
    }

    @Override
    public void close() throws IOException {
        hashTable.close();
    }
}
