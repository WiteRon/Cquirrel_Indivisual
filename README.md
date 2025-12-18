# Stream Query Engine

A real-time stream processing query engine built on Apache Flink, implementing TPC-H Query 3 for streaming data processing.

## Overview

This project implements a streaming version of TPC-H Query 3 using Apache Flink. It processes incremental data updates (INSERT/DELETE operations) and performs real-time revenue aggregation for customers in the AUTOMOBILE market segment.

## Environment Requirements

- **JDK 8+**
- **Maven 3.6+**
- **Python 3.9+**
- **DuckDB** (installed via pip)

## Project Structure

```mermaid
graph TD
    A[Project Root] --> B[src/main/java]
    A --> C[scripts]
    A --> D[pom.xml]
    A --> E[run.sh]
    A --> F[.env]
    
    B --> B1[com.streamengine.query]
    B1 --> B2[config/]
    B1 --> B3[domain/]
    B1 --> B4[engine/]
    B1 --> B5[processor/]
    B1 --> B6[sink/]
    B1 --> B7[util/]
    
    C --> C1[data/]
    C --> C2[data_generator.py]
    C --> C3[data_merger.py]
    C --> C4[result_validator.py]
    C --> C5[query3.sql]
    
    B2 --> B2a[QueryConfig]
    B2 --> B2b[DataPathConfig]
    B2 --> B2c[OperationEnum]
    
    B3 --> B3a[DataRecord]
    B3 --> B3b[ClientRecord]
    B3 --> B3c[OrderRecord]
    B3 --> B3d[ItemRecord]
    
    B4 --> B4a[QueryEngine]
    B4 --> B4b[StreamRouter]
    B4 --> B4c[RevenueCalculator]
    
    B5 --> B5a[ClientFilter]
    B5 --> B5b[OrderJoiner]
    B5 --> B5c[ItemJoiner]
    
    B6 --> B6a[ResultWriter]
    B6 --> B6b[AsyncWriter]
```

## Core Algorithm Flow

```mermaid
graph LR
    A[FileSource] --> B[StreamRouter]
    B --> C[ClientFilter]
    B --> D[OrderStream]
    B --> E[ItemStream]
    
    C --> F[OrderJoiner]
    D --> F
    F --> G[ItemJoiner]
    E --> G
    
    G --> H[RevenueCalculator]
    H --> I[ResultWriter]
    I --> J[Output CSV]
    
    style A fill:#e1f5ff
    style B fill:#fff4e1
    style C fill:#ffe1f5
    style F fill:#e1ffe1
    style G fill:#e1ffe1
    style H fill:#ffe1e1
    style I fill:#f5e1ff
    style J fill:#e1f5ff
```

**Processing Steps:**
1. **StreamRouter**: Splits input stream into customer, order, and lineitem streams
2. **ClientFilter**: Filters customers by market segment (AUTOMOBILE)
3. **OrderJoiner**: Joins filtered customers with orders using keyed state
4. **ItemJoiner**: Joins orders with lineitems, filters by ship date threshold
5. **RevenueCalculator**: Aggregates revenue incrementally (handles INSERT/DELETE)
6. **ResultWriter**: Outputs final results to CSV file

## Quick Start

### One-Command Launch

```bash
./run.sh
```

The script automatically:
1. Sets up Python virtual environment
2. Generates TPC-H test data (default ~20MB)
3. Compiles Java project with Maven
4. Runs Flink streaming job
5. Validates results against SQL query

### Custom Data Size

```bash
./run.sh 0.05  # Generate ~50MB data
```

### Output

Results are written to `scripts/data/output.csv`:
```
l_orderkey, o_orderdate, o_shippriority, revenue
48899, 1995-01-19, 0, 13272.0672
...
```
