# 快速开始指南

## 环境要求

- **JDK 21+**: 项目使用 Java 21 特性
- **Maven 3.9+**: 用于构建项目
- **Python 3.9+**: 用于数据生成和验证
- **磁盘空间**: 至少 100MB 可用空间

## 一键运行

最简单的方式是使用提供的运行脚本：

```bash
./run.sh
```

或者指定数据规模（scale factor）：

```bash
./run.sh 0.02  # 默认 20MB
./run.sh 0.1   # 约 100MB
./run.sh 0.5   # 约 500MB
```

## 运行脚本说明

`run.sh` 脚本会自动完成以下步骤：

1. **设置 Python 虚拟环境** (`.venv`)
   - 创建虚拟环境（如果不存在）
   - 安装 Python 依赖（duckdb）

2. **生成 TPC-H 数据**
   - 使用 DuckDB 的 dbgen 生成测试数据
   - 默认生成约 20MB 数据（scale_factor=0.02）

3. **合并数据文件**
   - 将 customer、orders、lineitem 表合并
   - 添加 INSERT/DELETE 操作标记
   - 生成 `tpch_q3.tbl` 文件

4. **编译 Java 项目**
   - Maven 清理和打包
   - 生成 fat jar: `target/stream-query-engine-1.0.0.jar`

5. **执行流处理作业**
   - 运行 Flink 作业
   - 处理数据并生成结果

6. **验证结果**
   - 使用 DuckDB 执行 SQL 查询
   - 对比流处理结果和数据库查询结果
   - 使用 `.venv` 中的 Python 环境

## 输出文件

运行完成后会生成以下文件：

- `scripts/data/tpch_q3.tbl`: 输入数据文件
- `scripts/data/output.csv`: 流处理结果
- `output.txt`: 执行日志
- `validation.txt`: 验证结果

## 手动执行步骤

如果需要分步执行：

### 1. 设置 Python 环境

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r scripts/requirements.txt
```

### 2. 生成数据

```bash
cd scripts
python data_generator.py 0.02  # 20MB
python data_merger.py
cd ..
```

### 3. 编译项目

```bash
mvn clean package
```

### 4. 运行应用

```bash
java --add-opens java.base/java.util=ALL-UNNAMED \
     --add-opens java.base/java.lang=ALL-UNNAMED \
     -jar target/stream-query-engine-1.0.0.jar
```

### 5. 验证结果

```bash
cd scripts
source ../.venv/bin/activate
python result_validator.py
cd ..
```

## 配置说明

配置文件 `.env` 包含以下可配置项：

```properties
# 数据文件路径
DATA_INPUT_FILE=./scripts/data/tpch_q3.tbl
DATA_OUTPUT_FILE=./scripts/data/output.csv

# 查询参数
MARKET_SEGMENT_FILTER=AUTOMOBILE
ORDER_DATE_THRESHOLD=1995-03-13
SHIP_DATE_THRESHOLD=1995-03-13
```

## 故障排查

### Java 版本问题

如果遇到 Java 版本错误，确保使用 JDK 21：

```bash
java -version  # 应该显示 21.x.x
```

### Python 依赖问题

如果验证脚本失败，确保虚拟环境已激活：

```bash
source .venv/bin/activate
pip install -r scripts/requirements.txt
```

### 数据文件不存在

确保已运行数据生成步骤：

```bash
cd scripts
python data_generator.py
python data_merger.py
```

### 内存不足

如果遇到内存错误，可以：

1. 减少数据规模：`./run.sh 0.01` (约 10MB)
2. 增加 JVM 内存：在 `run.sh` 中添加 `-Xmx2g`

## 性能提示

- 小规模测试：`scale_factor=0.01` (约 10MB)
- 中等规模：`scale_factor=0.02` (约 20MB，默认)
- 大规模测试：`scale_factor=0.1` (约 100MB)

## 验证结果解读

验证脚本会输出：

- ✅ **Results are identical**: 结果完全一致
- ❌ **Found in query but not in CSV**: 数据库查询有但流处理结果没有
- ❌ **Found in CSV but not in query**: 流处理结果有但数据库查询没有

正常情况下应该显示 "Results are identical"。

