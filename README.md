# Stream Query Engine

基于 Apache Flink 的实时流处理查询引擎，实现 TPC-H Query 3 的流式处理。

## 快速开始

### 前置要求

- JDK 8+
- Maven 3.6+
- Python 3.9+

### 一键启动

```bash
./run.sh
```

脚本会自动完成：
1. 设置 Python 虚拟环境
2. 生成 TPC-H 测试数据（默认 20MB）
3. 编译并运行流处理作业
4. 验证结果

### 自定义数据规模

```bash
./run.sh 0.05  # 生成约 50MB 数据
```

## 技术栈

- Java 8+
- Apache Flink 1.20.1
- Maven
- Python 3.9+ (DuckDB)

## 项目结构

```
├── src/main/java/        # Java 源代码
├── scripts/              # Python 脚本
│   ├── data/            # 数据目录（自动生成）
│   ├── data_generator.py
│   ├── data_merger.py
│   ├── result_validator.py
│   └── query3.sql
├── pom.xml
├── .env                  # 配置文件（可选）
└── run.sh                # 一键启动脚本
```

## 输出

结果文件：`scripts/data/output.csv`

格式：`l_orderkey, o_orderdate, o_shippriority, revenue`
