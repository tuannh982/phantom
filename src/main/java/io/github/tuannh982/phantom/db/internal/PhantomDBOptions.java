package io.github.tuannh982.phantom.db.internal;

import io.github.tuannh982.phantom.offheap.hashtable.hash.Hasher;
import io.github.tuannh982.phantom.offheap.hashtable.hash.Murmur3;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder(builderClassName = "Builder", buildMethodName = "build")
public class PhantomDBOptions {
    private final int maxKeySize;
    private final int dataFlushThreshold;
    private final int maxFileSize;
    private final int maxTombstoneFileSize;
    private final int numberOfIndexingThread;
    private final float compactionThreshold;
    private final Hasher hasher = new Murmur3();
    private final boolean offHeapHashTable;
    private final long estimatedMaxKeyCount;
    private final int memoryChunkSize;

    public static class Builder {
        void validate(boolean condition, String message) {
            if (!condition) {
                throw new IllegalArgumentException("validation failed at: " + message);
            }
        }

        public PhantomDBOptions build() {
            validate(maxKeySize > 0, "maxKeySize > 0");
            validate(dataFlushThreshold > 0, "dataFlushThreshold > 0");
            validate(maxFileSize > 0, "maxFileSize > 0");
            validate(maxTombstoneFileSize > 0, "maxTombstoneFileSize > 0");
            validate(numberOfIndexingThread > 0, "numberOfIndexingThread > 0");
            validate(compactionThreshold >= 0.0 && compactionThreshold <= 1.0, "compactionThreshold in interval(0,1)");
            if (offHeapHashTable) {
                validate(estimatedMaxKeyCount > 0, "estimatedMaxKeyCount > 0");
                validate(memoryChunkSize > 0, "memoryChunkSize > 0");
            }
            return new PhantomDBOptions(
                    maxKeySize,
                    dataFlushThreshold,
                    maxFileSize,
                    maxTombstoneFileSize,
                    numberOfIndexingThread,
                    compactionThreshold,
                    offHeapHashTable,
                    estimatedMaxKeyCount,
                    memoryChunkSize
            );
        }
    }
}
