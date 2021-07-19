package io.github.tuannh982.phantom.commons.io;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.regex.Pattern;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class FileUtils {
    public static void mkdir(File dir) throws IOException {
        if (dir.exists()) {
            if (!dir.isDirectory()) {
                throw new IOException(dir.getName() + " is not a directory");
            } else {
                return;
            }
        }
        boolean b = dir.mkdirs();
        if (!b) {
            throw new IOException("could not create directory " + dir.getName());
        }
    }

    public static void del(File f) throws IOException {
        if (f.isDirectory()) {
            File[] files = f.listFiles();
            if (files != null) {
                for (File file : files) {
                    del(file);
                }
            }
        } else {
            Files.delete(f.toPath());
        }
    }

    public static File[] ls(File dir, Pattern fileNamePattern) throws IOException {
        if (dir.isDirectory()) {
            if (fileNamePattern == null) {
                return dir.listFiles();
            } else {
                return dir.listFiles(file -> fileNamePattern.matcher(file.getName()).matches());
            }
        } else {
            throw new IOException(dir.getName() + " is not a directory");
        }
    }

    public static void copy(Path src, Path dest) throws IOException {
        Files.deleteIfExists(dest); // delete dest file
        try (
                FileChannel srcChannel = FileChannel.open(src, StandardOpenOption.READ);
                FileChannel destChannel = FileChannel.open(dest, StandardOpenOption.WRITE, StandardOpenOption.CREATE)
        ) {
            srcChannel.transferTo(0, srcChannel.size(), destChannel);
        }
    }

    public static int read(FileChannel channel, long pos, ByteBuffer buffer) throws IOException {
        long currentPos = pos;
        int bytesRead;
        do {
            bytesRead = channel.read(buffer, currentPos);
            currentPos += bytesRead;
        } while (bytesRead != -1 && buffer.hasRemaining());

        return (int)(currentPos - pos);
    }
}
