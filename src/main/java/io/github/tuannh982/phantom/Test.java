package io.github.tuannh982.phantom;

import io.github.tuannh982.phantom.db.DB;
import io.github.tuannh982.phantom.db.DBException;
import io.github.tuannh982.phantom.db.internal.PhantomDB;
import io.github.tuannh982.phantom.db.internal.PhantomDBOptions;
import io.github.tuannh982.phantom.db.policy.WritePolicy;

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
                         .numberOfIndexingThread(2 * Runtime.getRuntime().availableProcessors())
                         .compactionThreshold(0.5f)
                         .dataFlushThreshold(8 * 1024 * 1024)
                         .maxKeySize(8)
                         .maxFileSize(32 * 1024 * 1024)
                         .maxTombstoneFileSize(8 * 1024 * 1024)
                         .offHeapHashTable(true)
                         .estimatedMaxKeyCount(16)
                         .memoryChunkSize(4 * 1024 * 1024)
                         .build()
         );
         Random random = new Random(System.currentTimeMillis());
         byte[] key = new byte[] {1,2,3,4};
         if (true) {
             byte[] read = db.get(key).getValue();
             System.out.println(Arrays.toString(read));
             db.close();
             return;
         }
         byte[] tempValue = null;
         long start = System.currentTimeMillis();
         long putCount = 0;
         long delCount = 0;
         long readCount = 0;
         int putChance = 100;
         int iterations = 500_000;
         for (int i = 0; i < iterations; i++) {
             if (i % 500_000 == 0) {
                 System.out.println("iteration = " + i);
             }
             int choice = random.nextInt(100);
             if (choice < putChance) {
                 byte[] r = new byte[1 + random.nextInt(7)];
                 random.nextBytes(r);
                 db.put(key, r);
                 tempValue = r;
                 putCount++;
             } else {
                 db.delete(key);
                 tempValue = null;
                 delCount++;
             }
             byte[] read = db.get(key).getValue();
             readCount++;
             if (!Arrays.equals(read, tempValue)) {
                 System.out.println(
                         String.format(
                                 "read wrong at iteration %d, read = %s, actual = %s",
                                 i,
                                 Arrays.toString(read),
                                 Arrays.toString(tempValue)
                         )
                 );
                 return;
             }
         }
         // done
         long stop = System.currentTimeMillis();
         System.out.println(Arrays.toString(tempValue));
         db.close();
         double timeInSeconds = (double)(stop - start) / 1000;
         long totalOpCount = readCount + putCount + delCount;
         System.out.println(String.format("Single thread test, %d%% put, %d%% delete, read after put or delete", putChance, 100 - putChance));
         System.out.println(String.format("Run %d iterations", iterations));
         System.out.println(String.format("elapsed time = %.4f, total operations = %d, ops = %.4f",
                 timeInSeconds, totalOpCount, (double)totalOpCount / timeInSeconds)
         );
         System.out.println(String.format("read count = %d, put count = %d, delete count = %d",
                 readCount, putCount, delCount)
         );
         System.out.println(String.format("read/s = %.4f, put/s = %.4f, delete/s = %.4f",
                 (double)readCount/timeInSeconds, (double)putCount/timeInSeconds, (double)delCount/timeInSeconds)
         );
     }
}
