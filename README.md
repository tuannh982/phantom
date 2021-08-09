# Phantom

## Introduction
Phantom is an embedded key-value store, provides extreme high write throughput while maintains low latency data access.

Phantom was inspired by HaloDB, the name "Phantom" (belongs to the darkness) was derived from 
the name "Halo" (belongs to the light) from HaloDB.

The design principles of Phantom is old and simple. It uses log-structured data files and hash index (like HaloDB) to 
achieve the high write workload yet still maintain low latency access with the cost of no range scan support.

## Usage

### Installation

Add dependency to pom.xml (Maven)

```xml
<dependency>
    <groupId>io.github.tuannh982</groupId>
    <artifactId>phantom</artifactId>
    <version>0.1.2</version>
</dependency>
```

or build.gralde (Gradle)

```groovy
implementation group: 'io.github.tuannh982', name: 'phantom', version: '0.1.1'
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
byte[] read = db.get(key);
```
#### put
```java
byte[] key = new byte[] {...};
byte[] value = new byte[] {...};
boolean success = db.put(key, value);
```
#### putIfAbsent
```java
byte[] key = new byte[] {...};
byte[] value = new byte[] {...};
boolean success = db.putIfAbsent(key, value);
```
#### replace
```java
byte[] key = new byte[] {...};
byte[] value = new byte[] {...};
boolean success = db.replace(key, value);
```
#### delete
```java
byte[] key = new byte[] {...};
db.delete(key);
```

### Close DB instance
```java
db.close();
```

## TODOs
- Testing
  - Unit test
  - Performance test
  - Benchmark report
- Guide
  - Tuning guide
  - Development guide
- Support distributed mode
    - WAL
    - Replication manager