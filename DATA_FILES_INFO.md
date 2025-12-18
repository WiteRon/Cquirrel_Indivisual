# 数据文件信息

## 生成的数据文件

使用 `dbgen` 生成的数据文件（scale_factor=0.02，约20MB）：

| 文件 | 大小 | 说明 |
|------|------|------|
| `customer.tbl` | 472K | 客户表数据 |
| `lineitem.tbl` | 14M | 订单项表数据 |
| `orders.tbl` | 3.2M | 订单表数据 |
| `tpch_q3.tbl` | 20M | 合并后的输入文件（包含INSERT/DELETE操作） |
| **总计** | **38M** | 所有数据文件 |

## JAR文件

- `target/stream-query-engine-1.0.0.jar`: 64MB
  - 包含：Java代码 + Flink依赖库
  - **不包含**：数据文件（数据文件在运行时从 `scripts/data/` 目录读取）

## 数据文件位置

数据文件存储在：`scripts/data/`

运行时，Flink作业会从 `.env` 配置的路径读取：
```
DATA_INPUT_FILE=./scripts/data/tpch_q3.tbl
DATA_OUTPUT_FILE=./scripts/data/output.csv
```

## 数据生成

数据通过以下脚本生成：
1. `scripts/data_generator.py`: 使用DuckDB的dbgen生成TPC-H数据
2. `scripts/data_merger.py`: 合并数据并添加操作标记

生成时间：约3-4秒（20MB数据）
