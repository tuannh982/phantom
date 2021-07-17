package com.tuannh.phantom;

import com.tuannh.phantom.db.DB;
import com.tuannh.phantom.db.DBException;
import com.tuannh.phantom.db.internal.PhantomDB;
import com.tuannh.phantom.db.internal.PhantomDBOptions;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

@SuppressWarnings("all")
public class Test {
     public static void main(String[] args) throws DBException, IOException {
        String path = "/home/tuannh/Desktop/test";
        DB db = new PhantomDB(
                new File(path),
                PhantomDBOptions.builder()
                        .numberOfIndexingThread(Runtime.getRuntime().availableProcessors())
                        .compactionThreshold(0.5f)
                        .dataFlushThreshold(8 * 1024 * 1024)
                        .fixedKeySize(16)
                        .maxFileSize(32 * 1024 * 1024)
                .build()
        );
        Random random = new Random(System.currentTimeMillis());
        byte[] key = new byte[] {1,2,3,4};
        byte[] tempValue = null;
        boolean deleted = true;
        long start = System.currentTimeMillis();
        for (int i = 0; i < 5_000_000; i++) {
            if (i % 100_000 == 0) {
                System.out.println("iteration = " + i);
            }
            int choice = random.nextInt(10);
            // 8/10 chance to change value, 2/10 change to delete key
            if (choice < 8) {
                byte[] r = new byte[1 + random.nextInt(5)];
                random.nextBytes(r);
                if (deleted) {
                    db.putIfAbsent(key, r);
                    deleted = false;
                } else {
                    db.replace(key, r);
                }
                tempValue = r;
            } else {
                db.delete(key);
                tempValue = null;
                deleted = true;
            }
            db.get(key); // READ after write
        }
        // last write
        byte[] r = new byte[1 + random.nextInt(5)];
        random.nextBytes(r);
         if (deleted) {
             db.putIfAbsent(key, r);
             deleted = false;
         } else {
             db.replace(key, r);
         }
        tempValue = r;
        byte[] v = db.get(key);
        System.out.println(Arrays.toString(v));
        System.out.println(Arrays.toString(tempValue));
        long stop = System.currentTimeMillis();
        System.out.println("elapsed time = " + (double)(stop - start) / 1000 + " seconds");
        db.close();
    }
}
