# Stream Query Engine

实时流处理查询引擎，用于执行 TPC-H Query 3 的流式处理。

## 项目概述

本项目是一个基于 Apache Flink 的实时流处理系统，实现了 TPC-H Query 3 的流式查询处理。系统采用组件化架构设计，支持增量数据处理和实时聚合计算。

## 技术栈

- **Java 21**: 核心开发语言
- **Apache Flink 1.20.1**: 流处理框架
- **Maven**: 项目构建工具
- **Python 3.9+**: 数据生成和验证脚本
- **DuckDB**: 结果验证数据库

## 项目结构

```
new/
├── src/main/java/com/streamengine/query/
│   ├── config/          # 配置类
│   ├── domain/          # 领域模型
│   ├── engine/          # 核心引擎组件
│   ├── processor/       # 数据处理器
│   ├── state/           # 状态管理
│   ├── sink/            # 输出组件
│   └── util/            # 工具类
├── src/main/resources/  # 资源文件
├── scripts/             # Python 脚本
│   ├── data/            # 数据目录
│   ├── data_generator.py
│   ├── data_merger.py
│   ├── result_validator.py
│   └── query3.sql
├── pom.xml              # Maven 配置
├── .env                 # 环境配置（需要创建）
└── run.sh               # 运行脚本
```

## 快速开始

### 前置要求

- JDK 21+
- Maven 3.9+
- Python 3.9+
- 足够的磁盘空间（用于数据生成）

### 配置

1. 复制环境配置文件：
```bash
cp .env.example .env
```

2. 根据需要修改 `.env` 文件中的配置项：
```properties
DATA_INPUT_FILE=./scripts/data/tpch_q3.tbl
DATA_OUTPUT_FILE=./scripts/data/output.csv
MARKET_SEGMENT_FILTER=AUTOMOBILE
ORDER_DATE_THRESHOLD=1995-03-13
SHIP_DATE_THRESHOLD=1995-03-13
```

### 运行

执行一键运行脚本：
```bash
./run.sh
```

脚本会自动完成以下步骤：
1. 设置 Python 虚拟环境（`.venv`）
2. 生成 TPC-H 测试数据
3. 合并数据文件
4. 编译 Java 项目
5. 运行流处理作业
6. 验证结果

### 手动运行

如果需要分步执行：

1. **设置 Python 环境**：
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r scripts/requirements.txt
```

2. **生成数据**：
```bash
cd scripts
python data_generator.py
python data_merger.py
cd ..
```

3. **编译项目**：
```bash
mvn clean package
```

4. **运行应用**：
```bash
java --add-opens java.base/java.util=ALL-UNNAMED \
     --add-opens java.base/java.lang=ALL-UNNAMED \
     -jar target/stream-query-engine-1.0.0.jar
```

5. **验证结果**：
```bash
cd scripts
source ../.venv/bin/activate
python result_validator.py
cd ..
```

## 核心功能

### 1. 流式数据路由
- 从文件读取数据流
- 根据表类型自动路由到不同的处理流

### 2. 数据过滤
- 客户数据：过滤目标市场细分（AUTOMOBILE）
- 订单数据：过滤订单日期阈值
- 订单项数据：过滤发货日期阈值

### 3. 流式连接
- 客户-订单连接：基于客户键
- 订单-订单项连接：基于订单键
- 使用状态管理维护活跃记录

### 4. 增量聚合
- 支持 INSERT/DELETE 操作的增量更新
- 实时计算收入聚合
- 按订单键、订单日期、发货优先级分组

### 5. 异步输出
- 异步文件写入，提高性能
- 自动格式化输出结果

## 架构设计

### 数据流

```
FileSource 
  → StreamRouter (数据路由)
  → ClientFilter (客户过滤)
  → OrderJoiner (客户-订单连接)
  → ItemJoiner (订单-订单项连接)
  → RevenueCalculator (收入聚合)
  → ResultWriter (结果输出)
```

### 状态管理

- 使用 Flink 的 ValueState 和 MapState 管理状态
- 支持检查点和故障恢复
- 高效的状态访问和更新

### 组件化设计

- **配置层**: 统一管理配置参数
- **领域层**: 数据模型和业务逻辑
- **处理层**: 数据转换和处理
- **状态层**: 状态管理抽象
- **输出层**: 结果输出处理

## 输出格式

结果文件 (`output.csv`) 格式：
```csv
l_orderkey, o_orderdate, o_shippriority, revenue
48899, 1995-01-19, 0, 13272.0672
...
```

## 性能优化

- 使用异步文件写入减少 I/O 阻塞
- 状态管理优化，减少状态访问开销
- 并行处理支持
- 检查点机制保证容错性

## 故障排查

### 常见问题

1. **数据文件未找到**
   - 检查 `.env` 中的路径配置
   - 确保已运行数据生成脚本

2. **Python 依赖问题**
   - 确保虚拟环境已激活
   - 运行 `pip install -r scripts/requirements.txt`

3. **Java 版本不匹配**
   - 确保使用 JDK 21
   - 检查 `JAVA_HOME` 环境变量

4. **内存不足**
   - 增加 JVM 堆内存：`-Xmx2g`
   - 减少数据规模因子

## 开发说明

### 代码组织

- **包命名**: `com.streamengine.query.*`
- **设计模式**: 接口 + 实现类，组件化设计
- **状态管理**: 封装的状态管理器
- **异常处理**: 统一的异常处理机制

### 扩展性

- 易于添加新的数据源
- 支持自定义处理器
- 可配置的查询参数
- 模块化的组件设计

## 许可证

本项目仅供学习和研究使用。

## 联系方式

如有问题或建议，请提交 Issue。

