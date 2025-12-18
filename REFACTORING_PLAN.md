# 项目重构方案

## 一、架构设计原则

### 1.1 技术栈保持一致
- Java 21
- Apache Flink 1.20.1
- Maven
- Python 3.9+ (仅用于数据生成和验证)
- 不引入 Spring 等大型框架

### 1.2 重构目标
- 完全不同的包结构和命名
- 不同的设计模式和代码组织方式
- 不同的数据流处理架构
- 保留所有核心算法逻辑
- 功能完全一致

## 二、架构对比

### 原架构 (Cquirrel-UST)
```
包结构: cn.wxxlamp.ust.hk
- 基于 ProcessFunction 的流式处理
- 使用 KeyedCoProcessFunction 进行双流连接
- 使用 OutputTag 进行流分割
- 状态管理: ValueState + HashSet
- 实体类: BaseEntity 继承体系
```

### 新架构 (StreamQueryEngine)
```
包结构: com.streamengine.query
- 基于 RichFunction 的组件化处理
- 使用自定义连接器进行多流协调
- 使用广播流进行路由分发
- 状态管理: MapState + 自定义状态容器
- 数据模型: 基于接口的领域模型
```

## 三、详细设计方案

### 3.1 包结构重构

**原结构:**
```
cn.wxxlamp.ust.hk
├── constant/
├── entity/
├── exception/
├── function/
│   ├── aggregate/
│   ├── entity/
│   ├── groupby/
│   ├── input/
│   └── sink/
└── CquirrelJob.java
```

**新结构:**
```
com.streamengine.query
├── config/
│   ├── QueryConfig.java          (原 TpcHConstants)
│   ├── DataPathConfig.java       (原 FilePathConstants)
│   └── OperationEnum.java        (原 OperationType)
├── domain/
│   ├── DataRecord.java           (原 BaseEntity)
│   ├── ClientRecord.java         (原 Customer)
│   ├── OrderRecord.java          (原 Orders)
│   └── ItemRecord.java           (原 LineItem)
├── engine/
│   ├── QueryEngine.java          (原 CquirrelJob)
│   ├── StreamRouter.java         (原 SplitStreamFunction)
│   └── RevenueCalculator.java    (原 AggregateProcessFunction)
├── processor/
│   ├── ClientFilter.java         (原 CustomerProcessFunction)
│   ├── OrderJoiner.java          (原 OrdersProcessFunction)
│   └── ItemJoiner.java           (原 LineItemProcessFunction)
├── state/
│   ├── StateContainer.java       (状态管理抽象)
│   └── JoinStateManager.java     (连接状态管理)
├── sink/
│   ├── ResultWriter.java         (原 RevenueAggregateSink)
│   └── AsyncWriter.java          (原 AsyncFileWriter)
└── util/
    ├── DataParser.java
    └── ValidationUtil.java
```

### 3.2 设计模式重构

**原设计:**
- 继承体系: BaseEntity -> Customer/Orders/LineItem
- 函数式处理: ProcessFunction/KeyedProcessFunction
- 状态直接管理: ValueState<HashSet<T>>

**新设计:**
- 组合模式: DataRecord 接口 + 实现类
- 策略模式: 不同的 Processor 策略
- 状态封装: StateContainer 统一管理状态
- 责任链模式: 多阶段处理链

### 3.3 数据流架构重构

**原架构流程:**
```
FileSource -> SplitStreamFunction (OutputTag分割)
  -> CustomerStream (KeyedProcessFunction)
  -> OrdersStream (KeyedCoProcessFunction)
  -> LineItemStream (KeyedCoProcessFunction)
  -> AggregateStream (KeyedProcessFunction)
  -> Sink
```

**新架构流程:**
```
FileSource -> StreamRouter (广播流路由)
  -> ClientFilter (RichFilterFunction)
  -> OrderJoiner (RichCoFlatMapFunction)
  -> ItemJoiner (RichCoFlatMapFunction)
  -> RevenueCalculator (RichProcessFunction)
  -> ResultWriter
```

### 3.4 状态管理重构

**原方式:**
- ValueState<HashSet<Orders>>
- ValueState<Integer> counter
- ValueState<Customer> lastAlive

**新方式:**
- MapState<String, RecordSet> (使用 MapState 替代 HashSet)
- ReducingState<Integer> counter (使用 ReducingState)
- ListState<DataRecord> activeRecords (使用 ListState)

### 3.5 核心算法保留策略

**需要保留的核心逻辑:**
1. **客户过滤算法**: `c_mktsegment == 'AUTOMOBILE'`
2. **日期过滤算法**: `o_orderdate < '1995-03-13'` 和 `l_shipdate > '1995-03-13'`
3. **连接逻辑**: 基于 custkey 和 orderkey 的连接
4. **聚合算法**: `SUM(l_extendedprice * (1 - l_discount))`
5. **增量更新逻辑**: INSERT/DELETE 的增量聚合

**改写方式:**
- 算法逻辑保持不变，但实现方式不同
- 使用不同的数据结构和方法名
- 使用不同的控制流结构

### 3.6 代码风格重构

**命名风格:**
- 原: 驼峰命名，英文单词
- 新: 更描述性的命名，使用不同的词汇

**代码组织:**
- 原: 功能导向的包结构
- 新: 领域导向的包结构

**注释风格:**
- 原: 详细的步骤注释
- 新: 更简洁的文档注释

## 四、实现计划

### 阶段1: 基础结构搭建
1. 创建新的包结构
2. 定义配置类和枚举
3. 定义领域模型接口

### 阶段2: 核心组件实现
1. 实现 StreamRouter (数据路由)
2. 实现各个 Processor (处理逻辑)
3. 实现状态管理器

### 阶段3: 主流程整合
1. 实现 QueryEngine (主类)
2. 整合所有组件
3. 实现 Sink

### 阶段4: 测试和验证
1. 确保功能一致性
2. 性能对比
3. 代码审查

## 五、关键差异点

1. **包名完全不同**: `cn.wxxlamp.ust.hk` -> `com.streamengine.query`
2. **类名完全不同**: `CquirrelJob` -> `QueryEngine`, `Customer` -> `ClientRecord`
3. **设计模式不同**: 继承 -> 接口, 函数式 -> 组件化
4. **状态管理不同**: ValueState -> MapState/ListState
5. **数据流方式不同**: OutputTag -> 广播流
6. **代码风格不同**: 更现代的 Java 特性使用

## 六、文件清单

### Java 文件 (约 25 个)
- 配置类: 3 个
- 领域模型: 4 个
- 引擎组件: 3 个
- 处理器: 3 个
- 状态管理: 2 个
- Sink: 2 个
- 工具类: 2 个
- 异常类: 2 个
- 主类: 1 个
- 其他: 3 个

### Python 文件 (保持原样，仅重命名)
- `data_generator.py` (原 data_generate.py)
- `data_merger.py` (原 data_merge.py)
- `result_validator.py` (原 data_check.py)

### 配置文件
- `pom.xml` (完全不同的 groupId/artifactId)
- `log4j2.properties`
- `run.sh` (不同的脚本逻辑)

