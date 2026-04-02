# PGCastor Pro

**PGCastor Pro** 是一个用 C 语言编写的 PostgreSQL 数据复制和同步工具套件。它提供了从 PostgreSQL 实例捕获、转换和应用数据库变更的完整解决方案。

## 目录

- [项目概述](#项目概述)
- [系统架构](#系统架构)
- [功能特性](#功能特性)
- [快速开始](#快速开始)
- [构建指南](#构建指南)
- [部署指南](#部署指南)
- [命令参考](#命令参考)
- [配置说明](#配置说明)
- [目录结构](#目录结构)
- [依赖项](#依赖项)
- [支持的PostgreSQL版本](#支持的postgresql版本)
- [许可证](#许可证)

---

## 项目概述

### 用途
- 从 PostgreSQL WAL（预写日志）实时捕获数据
- 数据转换和 trail 文件生成
- 数据集成/应用到目标数据库
- 通过 CLI 工具和管理进程进行集中管理

### 技术栈

| 组件 | 技术 |
|------|------|
| 编程语言 | C (C99 标准) |
| 构建系统 | CMake 3.10+ |
| 解析器生成 | Bison + Flex |
| 数据库 | PostgreSQL 12+ |
| 压缩 | LZ4 |
| CLI库 | Readline |

---

## 系统架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           PGCASTOR 生态系统                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                  │
│  │   xscsci    │◄──►│  xmanager   │◄──►│  capture    │                  │
│  │  (命令行)   │    │  (管理器)   │    │  (捕获器)   │                  │
│  └─────────────┘    └─────────────┘    └─────────────┘                  │
│                            │                   │                        │
│                            │                   ▼                        │
│                            │           ┌─────────────┐                  │
│                            │           │ PostgreSQL  │                  │
│                            │           │  (源数据库) │                  │
│                            │           └─────────────┘                  │
│                            │                   │                        │
│                            │           WAL/Trail 文件                   │
│                            │                   │                        │
│                            │                   ▼                        │
│                            │           ┌─────────────┐                  │
│                            └──────────►│  integrate  │                  │
│                                        │  (集成器)   │                  │
│                                        └─────────────┘                  │
│                                              │                          │
│                                              ▼                          │
│                                        ┌─────────────┐                  │
│                                        │ PostgreSQL  │                  │
│                                        │ (目标数据库)│                  │
│                                        └─────────────┘                  │
└─────────────────────────────────────────────────────────────────────────┘
```

### 数据流向

```
1. 捕获阶段
   PostgreSQL ──WAL──► capture ──Trail文件──► 存储

2. 集成阶段
   存储 ──Trail文件──► integrate ──SQL──► 目标PostgreSQL

3. 管理阶段
   xscsci (CLI) ──命令──► xmanager ──控制──► capture/integrate
```

---

## 功能特性

- **实时WAL捕获**：从 PostgreSQL WAL 捕获 DML/DDL 操作
- **高性能**：多线程架构，支持并行处理
- **灵活过滤**：支持表的包含/排除模式
- **大事务处理**：特殊处理大型事务
- **在线刷新**：支持初始数据同步
- **DDL支持**：支持架构变更复制
- **冲突解决**：处理集成过程中的数据冲突
- **集中管理**：基于 CLI 的作业管理

---

## 快速开始

### 1. 构建项目

```bash
# 克隆仓库
git clone <仓库地址>
cd pgcastor

# 构建
mkdir -p build && cd build
cmake .. -DPOSTGRES_INSTALL_DIR=/path/to/pgsql
make -j4
```

### 2. 准备环境

```bash
# 设置环境变量
export LD_LIBRARY_PATH=/path/to/pgsql/lib:$(pwd)/lib:$(pwd)/interfaces/lib:$LD_LIBRARY_PATH
export PGCASTOR=$(pwd)/install
```

### 3. 启动服务

- **postgresql**
```bash
# 修改源端配置文件, 请确保源端数据库的wal_level为logical
sed -i 's/#\?wal_level\s*=.*/wal_level = logical/' data_source/postgresql.conf

# 修改完成后需要重启源端数据库
pg_ctl -D data_source restart
```

- **xscsci**启动 配置文件请参考 [主要配置文件](#主要配置文件)
```bash
# 启动xscsci
./bin/xscsci/xscsci

# 创建manager
xscsci=> create manager mgr1;
# 编辑manager配置文件
xscsci=> edit manager mgr1;
# 初始化manager
xscsci=> init manager mgr1;
# 启动manager
xscsci=> start manager mgr1;

# 创建capture
xscsci=> create capture cap1;
# 编辑capture配置文件
xscsci=> edit capture cap1;
# 初始化capture
xscsci=> init capture cap1;
# 启动capture
xscsci=> start capture cap1;

# 创建integrate
xscsci=> create integrate int1;
# 编辑integrate
xscsci=> edit integrate int1;
# 初始化integrate
xscsci=> init integrate int1;
# 启动integrate
xscsci=> start integrate int1;
```

### 4. 捕获数据
- 前序工作已经完成, 现在你可以在源端数据库执行dml和ddl命令, 在目标端查询是否正确执行成功了

---

## 构建指南

### 先决条件

| 要求 | 版本 |
|------|------|
| CMake | 3.10+ |
| GCC/Clang | 支持 C99 |
| PostgreSQL | 12+ |
| Readline | - |
| LZ4 | - |

### 构建步骤

```bash
# 1. 创建构建目录
mkdir -p build && cd build

# 2. CMake 配置 (Debug 构建)
cmake .. -DCMAKE_BUILD_TYPE=debug -DPOSTGRES_INSTALL_DIR=/path/to/pgsql

# 3. 编译
make -j4
```

### 构建目标

| 目标 | 类型 | 输出 |
|------|------|------|
| `castor_static` | 静态库 | `lib/libcastor_static.a` |
| `pgcastor` | 动态库 | `interfaces/lib/libpgcastor.so` |
| `capture` | 可执行文件 | `bin/capture/capture` |
| `integrate` | 可执行文件 | `bin/integrate/integrate` |
| `receivepglog` | 可执行文件 | `bin/receivepglog/receivepglog` |
| `xmanager` | 可执行文件 | `bin/xmanager/xmanager` |
| `xscsci` | 可执行文件 | `bin/xscsci/xscsci` |

### 构建类型

```bash
# Debug 构建 (开发环境)
cmake .. -DCMAKE_BUILD_TYPE=debug -DPOSTGRES_INSTALL_DIR=/path/to/pgsql

# Release 构建 (生产环境)
cmake .. -DCMAKE_BUILD_TYPE=release -DPOSTGRES_INSTALL_DIR=/path/to/pgsql
```

### 验证构建

```bash
# 检查可执行文件
ls -lh bin/capture/capture bin/integrate/integrate bin/xmanager/xmanager bin/xscsci/xscsci

# 检查库文件
ls -lh lib/libcastor_static.a interfaces/lib/libpgcastor.so

# 验证调试符号
file bin/capture/capture | grep "with debug_info"
```

### 清理构建

```bash
# 清理生成的文件
make clean_all

# 或删除整个构建目录
rm -rf build
```

---

## 部署指南

### 环境设置

```bash
# 添加到 ~/.bashrc 或 ~/.zshrc
export PGCASTOR=/path/to/pgcastor/install
export LD_LIBRARY_PATH=/path/to/pgsql/lib:$PGCASTOR/lib:$LD_LIBRARY_PATH
```

### PostgreSQL 要求

- 源数据库postgresql.conf 中设置 `wal_level = logical`

### 启动服务

#### 1. manager

```bash
# 启动xscsci
./bin/xscsci/xscsci

# 创建manager
xscsci=> create manager mgr1;
# 编辑manager配置文件
xscsci=> edit manager mgr1;
# 初始化manager
xscsci=> init manager mgr1;
# 启动manager
xscsci=> start manager mgr1;
```

#### 2. capture

```bash
# 启动xscsci
./bin/xscsci/xscsci

# 创建capture
xscsci=> create capture cap1;
# 编辑capture配置文件
xscsci=> edit capture cap1;
# 初始化capture
xscsci=> init capture cap1;
# 启动capture
xscsci=> start capture cap1;
```

#### 3. integrate

```bash
# 启动xscsci
./bin/xscsci/xscsci

# 创建integrate
xscsci=> create integrate int1;
# 编辑integrate
xscsci=> edit integrate int1;
# 初始化integrate
xscsci=> init integrate int1;
# 启动integrate
xscsci=> start integrate int1;
```

### 验证部署

```bash
# 检查进程
ps aux | grep -E "(xmanager|capture|integrate)" | grep -v grep

# 检查 xmanager 端口
ss -tlnp | grep your_cfg_port

# 检查数据目录
ls -la your_manager_cfg_data/ your_capture_cfg_data/ your_integrate_cfg_data/
```

### 测试数据同步

```bash
# 在源数据库创建测试表
/path/to/pgsql/bin/psql -p 5432 -d postgres -c "CREATE TABLE test_sync(id int);"

# 插入测试数据
/path/to/pgsql/bin/psql -p 5432 -d postgres -c "INSERT INTO test_sync VALUES (1);"

# 在目标数据库验证
/path/to/pgsql/bin/psql -p 5433 -d postgres -c "SELECT * FROM test_sync;"
```

---

## 解析库支持功能

### 数据类型

### DML

### DDL

---

## 命令参考

### xscsci CLI 命令

`xscsci` 工具提供了交互式 CLI 来管理 PGCastor 作业。

#### 启动 xscsci

```bash
./xscsci
```

#### 作业管理命令

| 命令 | 描述 |
|------|------|
| `create manager <名称>;` | 创建管理器作业 |
| `create capture <名称>;` | 创建捕获器作业 |
| `create integrate <名称>;` | 创建集成器作业 |
| `create progress <名称>;` | 创建进度监控作业 |

#### 生命周期命令

| 命令 | 描述 |
|------|------|
| `init manager <名称>;` | 初始化管理器 |
| `init capture <名称>;` | 初始化捕获器 |
| `init integrate <名称>;` | 初始化集成器 |
| `start manager <名称>;` | 启动管理器 |
| `start capture <名称>;` | 启动捕获器 |
| `start integrate <名称>;` | 启动集成器 |
| `start all;` | 启动所有任务 |
| `stop manager <名称>;` | 停止管理器 |
| `stop capture <名称>;` | 停止捕获器 |
| `stop integrate <名称>;` | 停止集成器 |
| `stop all;` | 停止所有任务 |

### 编辑配置文件命令

| 命令 | 描述 |
|------|------|
| `edit manager <名称>;` | 编辑管理器配置文件 |
| `edit capture <名称>;` | 编辑捕获器配置文件 |
| `edit integrate <名称>;` | 编辑集成器配置文件 |


#### 信息查询命令

| 命令 | 描述 |
|------|------|
| `info manager <名称>;` | 显示管理器信息 |
| `info capture <名称>;` | 显示捕获器信息 |
| `info integrate <名称>;` | 显示集成器信息 |
| `info progress <名称>;` | 显示进度监控器信息 |
| `info all;` | 显示所有信息 |
| `watch <类型> <名称>;` | 实时监控作业状态 |

#### 其他命令

| 命令 | 描述 |
|------|------|
| `remove <类型> <名称>;` | 删除配置文件 |
| `drop <类型> <名称>;` | 删除作业 |
| `alter progress <名称> add;` | 添加进度监控成员 |
| `alter progress <名称> remove;` | 移除进度监控成员 |
| `help` | 显示帮助 |
| `exit` | 退出 xscsci |

---

## 配置说明

### 配置格式

INI 风格的键值对配置文件：

```ini
jobname = capture
log_dir = /path/to/log
log_level = info
data = /path/to/capturedata
url = "port=5432 dbname=postgres user=postgres"
dbtype = postgres
dbversion = 12
catalog_schema = pgcastor
```

### 主要配置文件样例

| 文件 | 用途 | 说明 |
|------|------|------|
| `etc/sample/capture.cfg.sample` | 捕获器配置 | 从源数据库捕获WAL变更 |
| `etc/sample/integrate.cfg.sample` | 集成器配置 | 将变更应用到目标数据库 |
| `etc/sample/xmanager.cfg.sample` | 管理器配置 | 管理和协调所有作业 |
| `etc/sample/receivelog.cfg.sample` | WAL接收器配置 | 接收和归档WAL日志 |

---

### capture.cfg.sample 参数

捕获器配置文件，用于从源数据库捕获变更。

#### 基础配置

| 参数 | 描述 | 示例值 | 必填 |
|------|------|--------|------|
| `jobname` | 作业名称 | `capture` | ✓ |
| `log_dir` | 日志目录（必须存在，未配置时使用 `data/log`） | `/opt/pgcastor/log` | |
| `log_level` | 日志级别（`debug`/`info`/`warning`/`error`） | `info` | |
| `data` | 工作目录 | `/opt/pgcastor/capturedata` | ✓ |

#### 数据库连接

| 参数 | 描述 | 示例值 | 必填 |
|------|------|--------|------|
| `url` | 数据库连接字符串 | `"port=5432 dbname=postgres user=postgres"` | ✓ |
| `wal_dir` | PostgreSQL WAL目录 | `/opt/postgresql/data/pg_wal` | ✓ |
| `dbtype` | 数据库类型（目前支持 `postgres`） | `postgres` | ✓ |
| `dbversion` | PostgreSQL 版本（`12`/`13`/`14`） | `12` | ✓ |

#### 同步配置

| 参数 | 描述 | 示例值 | 必填 |
|------|------|--------|------|
| `catalog_schema` | 同步状态表所在模式 | `pgcastor` | ✓ |
| `ddl` | 是否同步 DDL（`1`=开，`0`=关） | `1` | |
| `table` | 要同步的表（格式：`schema.table`） | `"public.test"` | ✓ |
| `tableexclude` | 排除的表（可多行） | `*.*` | |
| `addtablepattern` | 通过 CREATE TABLE 自动捕获的表模式 | `"public.%"` | |

#### 性能调优

| 参数 | 描述 | 示例值 | 必填 |
|------|------|--------|------|
| `trail_max_size` | Trail 文件大小（MB） | `16` | ✓ |
| `capture_buffer` | 捕获作业最大内存使用（MB） | `8192` | ✓ |
| `max_work_per_refresh` | 现有数据同步的线程数 | `10` | ✓ |
| `max_page_per_refreshsharding` | 现有表分片阈值 | `10000` | ✓ |

#### 初始同步

| 参数 | 描述 | 示例值 | 必填 |
|------|------|--------|------|
| `refreshstragety` | 是否同步现有数据（`0`=关/`1`=开） | `0` | |

#### 其他选项

| 参数 | 描述 | 示例值 | 必填 |
|------|------|--------|------|
| `compress_algorithm` | 压缩算法（现有数据同步，支持 `bzip2`） | `bzip2` | |
| `compatibility` | 兼容版本（Trail 文件格式） | `10` | |

---

### integrate.cfg.sample 参数

集成器配置文件，用于将捕获的变更应用到目标数据库。

#### 基础配置

| 参数 | 描述 | 示例值 | 必填 |
|------|------|--------|------|
| `jobname` | 作业名称 | `integrate` | ✓ |
| `log_dir` | 日志目录（必须存在，未配置时使用 `data/log`） | `/opt/pgcastor/log` | |
| `log_level` | 日志级别（`info`/`debug`） | `info` | |
| `data` | 工作目录 | `/opt/pgcastor/integratedata` | ✓ |

#### 数据库连接

| 参数 | 描述 | 示例值 | 必填 |
|------|------|--------|------|
| `url` | 目标数据库连接字符串 | `"port=5438 dbname=postgres user=postgres"` | ✓ |

#### Trail 文件

| 参数 | 描述 | 示例值 | 必填 |
|------|------|--------|------|
| `trail_dir` | Trail 文件目录（应为 capture 的 data 目录） | `/opt/pgcastor/capturedata` | ✓ |
| `trail_max_size` | Trail 文件大小（MB，**必须与 capture 一致**） | `16` | ✓ |

#### 同步配置

| 参数 | 描述 | 示例值 | 必填 |
|------|------|--------|------|
| `catalog_schema` | 初始化时的状态表模式（建议创建独立模式） | `pgcastor` | ✓ |

#### 性能调优

| 参数 | 描述 | 示例值 | 必填 |
|------|------|--------|------|
| `integrate_buffer` | 大事务分片内存（MB） | `128` | ✓ |
| `max_work_per_refresh` | 现有数据应用的线程数 | `10` | ✓ |
| `integrate_method` | 集成应用模式（`burst`/`empty`） | `burst` | |

#### 事务处理

| 参数 | 描述 | 示例值 | 必填 |
|------|------|--------|------|
| `mergetxn` | 是否合并事务（`0`=否/`1`=是，未配置默认 `0`） | `1` | |
| `txbundlesize` | 事务合并阈值——每个事务的语句数（范围：0~10000000） | `1000` | |

#### 数据处理

| 参数 | 描述 | 示例值 | 必填 |
|------|------|--------|------|
| `truncate_table` | 应用现有数据时是否清空表（`0`=否/`1`=是） | `0` | |
| `compress_algorithm` | 压缩算法（**必须与 capture 配置一致**，`bunzip2`） | `bunzip2` | |
| `compress_level` | 压缩级别 | `9` | |

#### 其他选项

| 参数 | 描述 | 示例值 | 必填 |
|------|------|--------|------|
| `compatibility` | 兼容版本 | `10` | |

---

### xmanager.cfg.sample 参数

管理器配置文件，用于管理和协调所有 PGCastor 作业。

#### 基础配置

| 参数 | 描述 | 示例值 | 必填 |
|------|------|--------|------|
| `jobname` | 作业名称 | `xmanager` | ✓ |
| `log_dir` | 日志目录（必须存在，未配置时使用 `data/log`） | `/opt/pgcastor/log` | |
| `log_level` | 日志级别（`info`/`debug`） | `info` | |
| `data` | 工作目录 | `/opt/pgcastor/xmanagerdata` | ✓ |

#### 网络配置

| 参数 | 描述 | 示例值 | 必填 |
|------|------|--------|------|
| `host` | xmanager 本地 IP 地址 | `127.0.0.1` | ✓ |
| `port` | 网络端口号 | `6543` | ✓ |

#### TCP 保活机制

| 参数 | 描述 | 示例值 | 必填 |
|------|------|--------|------|
| `tcp_keepalive` | 启用 TCP 保活机制（`0`=关/`1`=开） | `1` | |
| `tcp_user_timeout` | 等待对端 ACK 的超时时间（毫秒，范围：0~300000） | `60000` | |
| `tcp_keepalives_idle` | TCP 连接无活动后发送保活探测前的秒数（范围：0~300） | `30` | |
| `tcp_keepalives_interval` | 保活探测间隔秒数（范围：0~300） | `3` | |
| `tcp_keepalives_count` | 保活探测次数（范围：0~100） | `10` | |

#### 其他选项

| 参数 | 描述 | 示例值 | 必填 |
|------|------|--------|------|
| `unixdomaindir` | Unix 域套接字目录 | `/tmp` | |

---

### receivelog.cfg.sample 参数

WAL 接收器配置文件，用于接收和归档 PostgreSQL WAL 日志。

#### 日志配置

| 参数 | 描述 | 示例值 | 必填 |
|------|------|--------|------|
| `log_level` | 日志级别 | `info` | |
| `log_dir` | 日志目录 | `/opt/receivewaldata/log` | |

#### 工作目录

| 参数 | 描述 | 示例值 | 必填 |
|------|------|--------|------|
| `data` | 工作目录/事务日志目录 | `/opt/receivewaldata` | ✓ |

#### 同步参数

| 参数 | 描述 | 示例值 | 必填 |
|------|------|--------|------|
| `timeline` | 同步起始时间线 | `1` | |
| `startpos` | 同步起始 LSN | `"0/02000000"` | |

#### 连接配置

| 参数 | 描述 | 示例值 | 必填 |
|------|------|--------|------|
| `primary_conn_info` | 源数据库连接字符串 | `"port=9527 dbname=postgres user=postgres"` | ✓ |

#### 其他选项

| 参数 | 描述 | 示例值 | 必填 |
|------|------|--------|------|
| `restore_command` | 日志缺失时从源端复制的命令（`%f` `%p` 为固定格式） | `"scp user@ip:/opt/pg/archive/%f %p"` | |
| `slot_name` | 复制槽名称（必须存在于数据库中，可为空） | `"recvwal"` | |

---

### 配置注意事项

#### 1. capture 和 integrate 参数对应关系

以下参数在 capture 和 integrate 中必须保持一致：

| 参数 | 说明 |
|------|------|
| `trail_max_size` | Trail 文件大小必须完全一致 |
| `compress_algorithm` | 压缩算法必须对应（capture 用 `bzip2`，integrate 用 `bunzip2`） |

#### 2. 性能调优建议

- **capture_buffer**：建议设置为系统可用内存的 50%-70%
- **max_work_per_refresh**：根据 CPU 核心数设置，建议为核心数的 1-2 倍
- **txbundlesize**：事务合并阈值越大性能越好，但故障恢复时间更长

#### 3. 常见配置错误

- ❌ capture 和 integrate 的 `trail_max_size` 不一致
- ❌ 忘记创建 `log_dir` 目录导致启动失败
- ❌ `wal_dir` 路径错误导致无法读取 WAL
- ❌ `table` 参数格式错误（应为 `schema.table`）
- ❌ `catalog_schema` 模式不存在

---

## 目录结构

```
pgcastor/
├── bin/                   # 可执行文件和源码
│   ├── capture/           # 捕获器组件
│   ├── integrate/         # 集成器组件
│   ├── receivepglog/      # WAL接收器
│   ├── xmanager/          # 管理器组件
│   └── xscsci/            # CLI工具
├── src/                   # 核心库源码
│   ├── catalog/           # 目录管理
│   ├── command/           # 命令处理
│   ├── elog/              # 日志系统
│   ├── net/               # 网络层
│   ├── parser/            # TRAIL解析
│   ├── utils/             # 工具函数
│   └── ...
├── incl/                  # 头文件
├── interfaces/            # 动态库
│   ├── src/              # 接口源码
│   ├── incl/             # 接口头文件
│   └── lib/              # libpgcastor.so
├── parser/               # wal解析器
├── lib/                  # libcastor_static.a
├── etc/                  # 配置模板
└── build/                # 构建目录
```

---

## 依赖项

### 必需依赖

| 依赖 | 用途 |
|------|------|
| PostgreSQL (libpq) | 数据库客户端库 |
| Readline | 命令行编辑 |
| LZ4 | 压缩 |
| Threads (pthread) | 线程 |
| Math (libm) | 数学运算 |

### 可选依赖

| 依赖 | 用途 |
|------|------|
| Bison | 解析器生成（用于xscsci）|
| Flex | 词法分析器生成（用于xscsci）|

### 安装依赖

```bash
# Ubuntu/Debian
sudo apt-get install libreadline-dev liblz4-dev bison flex

# CentOS/RHEL
sudo yum install readline-devel lz4-devel bison flex

# PostgreSQL 开发包
sudo apt-get install postgresql-server-dev-12  # Ubuntu
sudo yum install postgresql12-devel            # CentOS
```

---

## 支持的PostgreSQL版本

| 版本 | 状态 |
|------|------|
| PostgreSQL 12 | 支持 |
| PostgreSQL 13 | 计划中 |
| PostgreSQL 14 | 计划中 |

---

## 故障排除

### 进程无法启动

1. 检查日志：`cat xdata/log/*.log`
2. 检查PostgreSQL连接：`psql -p 5432 -c "SELECT 1;"`
3. 检查权限：`ls -la /path/to/pg_wal/`

### 数据未同步

1. 验证所有进程运行：`ps aux | grep -E "(xmanager|capture|integrate)"`
2. 检查trail文件：`ls -la capturedata/trail/`

### 端口被占用

```bash
# 查找并终止进程
ss -tlnp | grep 6543
kill -9 <PID>
```

---

## 许可证

参考LICENSE
Copyright (c) 2024-2026, Byte Sync Development Group

---

## 贡献指南

参考CONTRIBUTING.md

---

