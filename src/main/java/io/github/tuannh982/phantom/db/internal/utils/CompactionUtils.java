package io.github.tuannh982.phantom.db.internal.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Map;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CompactionUtils {
    public static int recordStaleData(Map<Integer, Integer> staleDataMap, int fileId, int recordSize) {
        Integer i = staleDataMap.get(fileId);
        int staleData = i == null ? 0 : i;
        staleDataMap.put(fileId, staleData + recordSize);
        return staleData + recordSize;
    }
}
