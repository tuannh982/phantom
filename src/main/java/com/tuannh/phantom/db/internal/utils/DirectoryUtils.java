package com.tuannh.phantom.db.internal.utils;

import com.tuannh.phantom.commons.io.FileUtils;
import com.tuannh.phantom.db.DBException;
import com.tuannh.phantom.db.internal.DBDirectory;
import com.tuannh.phantom.db.internal.PhantomDBOptions;
import com.tuannh.phantom.db.internal.file.DBFile;
import com.tuannh.phantom.db.internal.file.TombstoneFile;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DirectoryUtils {
    public static final Pattern DATA_FILE_PATTERN = Pattern.compile("([0-9]+)\\.datac?");
    public static final Pattern INDEX_FILE_PATTERN = Pattern.compile("([0-9]+)\\.index");
    public static final Pattern TOMBSTONE_FILE_PATTERN = Pattern.compile("([0-9]+)\\.tombstone");
    public static final Pattern STORAGE_FILE_PATTERN = Pattern.compile("([0-9]+)\\.[a-z]+");

    // all storage file name is number
    public static int fileId(File file, Pattern pattern) {
        Matcher matcher = pattern.matcher(file.getName());
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        }
        throw new IllegalArgumentException("Cannot extract file id for file " + file.getPath());
    }

    public static File[] dataFiles(File dir) {
        try {
            File[] files = FileUtils.ls(dir, DATA_FILE_PATTERN);
            Arrays.sort(files, Comparator.comparingInt(file -> fileId(file, DATA_FILE_PATTERN)));
            return files;
        } catch (IOException ignored) { /*never happen*/ }
        return new File[0];
    }

    public static File[] indexFiles(File dir) {
        try {
            File[] files = FileUtils.ls(dir, INDEX_FILE_PATTERN);
            Arrays.sort(files, Comparator.comparingInt(file -> fileId(file, INDEX_FILE_PATTERN)));
            return files;
        } catch (IOException ignored) { /*never happen*/ }
        return new File[0];
    }

    public static File[] tombstoneFiles(File dir) {
        try {
            File[] files = FileUtils.ls(dir, TOMBSTONE_FILE_PATTERN);
            Arrays.sort(files, Comparator.comparingInt(file -> fileId(file, TOMBSTONE_FILE_PATTERN)));
            return files;
        } catch (IOException ignored) { /*never happen*/ }
        return new File[0];
    }

    public static Map.Entry<Map<Integer, DBFile>, Integer> buildDataFileMap(DBDirectory dbDirectory, PhantomDBOptions options, int defaultFileId) throws IOException, DBException {
        // ---------------------Prep   ----------------------------------------------------------------
        Map<Integer, DBFile> dbFileMap = new ConcurrentHashMap<>();
        int maxFileId = Integer.MIN_VALUE;
        // ---------------------STAGE 1----------------------------------------------------------------
        File[] dataFiles = dbDirectory.dataFiles();
        for (File file : dataFiles) {
            String fileName = file.getName();
            boolean compacted = fileName.charAt(fileName.length() - 1) == 'c';
            Matcher fileIdMatcher = DATA_FILE_PATTERN.matcher(fileName);
            if (fileIdMatcher.find()) {
                int fileId = Integer.parseInt(fileIdMatcher.group(1));
                // update maxFileId
                maxFileId = Math.max(maxFileId, fileId);
                DBFile dbFile = DBFile.openForRead(fileId, dbDirectory, options, compacted);
                if (dbFileMap.putIfAbsent(fileId, dbFile) != null) {
                    throw new DBException("found duplicated file ID, file_id = " + fileId);
                }
            } else {
                throw new DBException("filename did not match data file pattern"); // never happen
            }
        }
        // ---------------------STAGE 2----------------------------------------------------------------
        if (maxFileId == Integer.MIN_VALUE) {
            maxFileId = defaultFileId;
        }
        return new AbstractMap.SimpleImmutableEntry<>(dbFileMap, maxFileId);
    }

    public static void repairLatestDataFile(Map<Integer, DBFile> dataFileMap) throws IOException {
        int maxDataFileId = Integer.MIN_VALUE;
        int maxCompactedDataFileId = Integer.MIN_VALUE;
        for (Map.Entry<Integer, DBFile> entry : dataFileMap.entrySet()) {
            if (entry.getValue().isCompacted()) {
                maxCompactedDataFileId = Math.max(maxCompactedDataFileId, entry.getKey());
            } else {
                maxDataFileId = Math.max(maxDataFileId, entry.getKey());
            }
        }
        if (maxDataFileId != Integer.MIN_VALUE) {
            DBFile latestDataFile = dataFileMap.get(maxDataFileId).repairAndOpenForRead();
            dataFileMap.put(maxDataFileId, latestDataFile);
        }
        if (maxCompactedDataFileId != Integer.MIN_VALUE) {
            DBFile latestCompactedDataFile = dataFileMap.get(maxCompactedDataFileId).repairAndOpenForRead();
            dataFileMap.put(maxCompactedDataFileId, latestCompactedDataFile);
        }
    }

    public static void repairLatestTombstoneFile(DBDirectory dbDirectory, PhantomDBOptions options) throws IOException {
        File[] tombstoneFiles = dbDirectory.tombstoneFiles();
        File latestFile = tombstoneFiles[tombstoneFiles.length - 1];
        int fileId = fileId(latestFile, TOMBSTONE_FILE_PATTERN);
        TombstoneFile tombstoneFile = TombstoneFile.open(fileId, dbDirectory, options);
        TombstoneFile repaired = tombstoneFile.repairAndOpen();
        repaired.close();
    }
}
