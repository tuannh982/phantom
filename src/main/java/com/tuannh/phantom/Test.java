package com.tuannh.phantom;

import com.tuannh.phantom.db.DB;
import com.tuannh.phantom.db.DBException;
import com.tuannh.phantom.db.internal.PhantomDB;
import com.tuannh.phantom.db.internal.PhantomDBOptions;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class Test {
    public static void main(String[] args) throws DBException, IOException {
        String path = "/home/tuannh/Desktop/test";
        DB db = new PhantomDB(
                new File(path),
                PhantomDBOptions.builder()
                        .numberOfIndexingThread(2)
                        .compactionThreshold(0.5f)
                        .dataFlushThreshold(4 * 1024 * 1024)
                        .fixedKeySize(16)
                        .maxFileSize(1024 * 1024 * 1024)
                .build()
        );
        byte[] key1 = new byte[] {1,2,3,4};
        byte[] value1 = new byte[] {99};
        byte[] value2 = new byte[] {100};
        //--
//        boolean b = db.putIfAbsent(key1, value1);
//        System.out.println(b);
        byte[] v = db.get(key1);
        System.out.println(Arrays.toString(v));
//        //--
//        boolean b1 = db.putIfAbsent(key1, value2);
//        System.out.println(b1);
//        byte[] v1 = db.get(key1);
//        System.out.println(Arrays.toString(v1));
//        //--
//        boolean b2 = db.replace(key1, value2);
//        System.out.println(b2);
//        byte[] v2 = db.get(key1);
//        System.out.println(Arrays.toString(v2));
        db.close(); // TODO fix bug closed channel
    }
}
