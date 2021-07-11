package com.tuannh.phantom.db.internal.utils;

import com.tuannh.phantom.commons.io.FileUtils;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DirectoryUtils {
    private static final Pattern DATA_FILE_PATTERN = Pattern.compile("([0-9]+)\\.datac?");
    private static final Pattern INDEX_FILE_PATTERN = Pattern.compile("([0-9]+)\\.index");
    private static final Pattern TOMBSTONE_FILE_PATTERN = Pattern.compile("([0-9]+)\\.tombstone");
    private static final Pattern STORAGE_FILE_PATTERN = Pattern.compile("([0-9]+)\\.[a-z]+");

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
}
