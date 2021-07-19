package com.github.tuannh982.phantom;

import com.github.tuannh982.phantom.db.DB;
import com.github.tuannh982.phantom.db.DBException;
import com.github.tuannh982.phantom.db.internal.PhantomDB;
import com.github.tuannh982.phantom.db.internal.PhantomDBOptions;

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
                         .maxTombstoneFileSize(8 * 1024 * 1024)
                         .build()
         );
         Random random = new Random(System.currentTimeMillis());
         byte[] key = new byte[] {1,2,3,4};
         byte[] tempValue = null;
         long start = System.currentTimeMillis();
         for (int i = 0; i < 5_000_000; i++) {
             if (i % 500_000 == 0) {
                 System.out.println("iteration = " + i);
             }
             int choice = random.nextInt(100);
             if (choice < 80) {
                 byte[] r = new byte[1 + random.nextInt(7)];
                 random.nextBytes(r);
                 db.put(key, r);
                 tempValue = r;
             } else {
                 db.delete(key);
                 tempValue = null;
             }
             byte[] read = db.get(key);
             if (!Arrays.equals(read, tempValue)) {
                 System.out.println("read wrong, read = " + Arrays.toString(read) + ", actual = " + Arrays.toString(tempValue));
                 return;
             }
         }
         // done
         System.out.println(Arrays.toString(tempValue));
         long stop = System.currentTimeMillis();
         System.out.println("elapsed time = " + (double)(stop - start) / 1000 + " seconds");
         db.close();
     }
}
