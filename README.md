# Phantom

## Introduction
Phantom is an embedded key-value store, provides extreme high write throughput while maintains low latency data access.

Phantom was inspired by HaloDB, the name "Phantom" (belongs to the darkness) was derived from 
the name "Halo" (belongs to the light) from HaloDB.

The design principles of Phantom is old and simple. It uses log-structured data files and hash index (like HaloDB) to 
achieve the high write workload yet still maintain low latency access with the cost of no range scan support.

## TODOs
- Testing
  - Unit test
  - Performance test
  - Benchmark report
- Guide
  - Tuning guide
  - Development guide
- Transaction manager
- Support distributed mode
    - WAL
    - Replication manager