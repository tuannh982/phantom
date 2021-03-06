Phantom
======

[![GitHub](https://img.shields.io/github/license/tuannh982/phantom.svg)](https://github.com/tuannh982/phantom/blob/master/LICENSE)
[![Total Alerts](https://img.shields.io/lgtm/alerts/g/tuannh982/phantom.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/tuannh982/phantom/alerts)
[![Code Quality: Java](https://img.shields.io/lgtm/grade/java/g/tuannh982/phantom.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/tuannh982/phantom/context:java)

## Introduction
Phantom is an embedded key-value store, provides extreme high write throughput while maintains low latency data access.

Phantom was inspired by HaloDB, the name "Phantom" (belongs to the darkness) was derived from 
the name "Halo" (belongs to the light) from HaloDB.

The design principles of Phantom is old and simple. It uses log-structured data files and hash index (like HaloDB) to 
achieve the high write workload yet still maintain low latency access with the cost of no range scan support.

## Usage

### Installation (PRE-RELEASE version)

Add dependency to pom.xml (Maven)

```xml
<dependency>
    <groupId>io.github.tuannh982</groupId>
    <artifactId>phantom</artifactId>
</dependency>
```

or build.gralde (Gradle)

```groovy
implementation 'io.github.tuannh982:phantom:+'
```

### Create DB instance
```java
String path = "/path/to/your/db/dir";
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
```

### Basic operations
#### get
```java
byte[] key = new byte[] {...};
GetResult result = db.get(key); 
byte[] read = result.getValue();
```
#### put
```java
byte[] key = new byte[] {...};
byte[] value = new byte[] {...};
ModifyResult result = db.put(key, value);
boolean success = result.isSuccess();
```
#### putIfAbsent
```java
byte[] key = new byte[] {...};
byte[] value = new byte[] {...};
ModifyResult result = db.putIfAbsent(key, value);
boolean success = result.isSuccess();
```
#### replace
```java
byte[] key = new byte[] {...};
byte[] value = new byte[] {...};
ModifyResult result = db.replace(key, value);
boolean success = result.isSuccess();
```
#### delete
```java
byte[] key = new byte[] {...};
ModifyResult result = db.delete(key, value);
boolean success = result.isSuccess();
```

#### advanced write operation
```java
byte[] key = new byte[] {...};
WriteOps ops = WriteOps.PUT;
WritePolicy policy = WritePolicy.builder()
        .sequenceNumberPolicy(WritePolicy.SequenceNumberPolicy.NONE)
        .recordExistsAction(WritePolicy.RecordExistsAction.CREATE_ONLY)
        .build();
ModifyResult result = db.write(ops, policy, key, value);
boolean success = result.isSuccess();
```

### Close DB instance
```java
db.close();
```

## Notes

This project still in development, so there are lots of bugs exist.
Please don't use the pre-release version as they contain a lot of bugs, 
use directly from master branch since it's always up-to-date and maybe contains bug fixed.

## TODOs
- Testing
  - Unit test
  - Performance test
  - Benchmark report
- Guide
  - Tuning guide
  - Development guide
- Support distributed mode
    - WAL (in consideration)
    - Replication manager