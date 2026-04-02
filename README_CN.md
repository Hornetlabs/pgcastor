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
cd pgcastor-pro

# 构建
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=debug -DPOSTGRES_INSTALL_DIR=/path/to/pgsql
make -j4
```

### 2. 准备环境

```bash
# 设置环境变量
export LD_LIBRARY_PATH=/path/to/pgsql/lib:$(pwd)/lib:$(pwd)/interfaces/lib:$LD_LIBRARY_PATH
export PGCASTOR=$(pwd)/install
```

### 3. 启动服务

```bash
# 创建并启动管理器
./bin/xscsci/xscsci
xscsci=> create manager mgr1
xscsci=> init manager mgr1
xscsci=> start manager mgr1

# 创建并启动捕获器
xscsci=> create capture cap1
xscsci=> init capture cap1
xscsci=> start capture cap1

# 创建并启动集成器
xscsci=> create integrate int1
xscsci=> init integrate int1
xscsci=> start integrate int1
```

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
export PGCASTOR=/path/to/pgcastor-pro/install
export LD_LIBRARY_PATH=/path/to/pgsql/lib:$PGCASTOR/../lib:$PGCASTOR/../interfaces/lib:$LD_LIBRARY_PATH
```

### PostgreSQL 要求

- 源数据库：端口 5432（默认）
- 目标数据库：端口 5433（或按配置）
- postgresql.conf 中设置 `wal_level = logical`
- `max_replication_slots` >= 1

### 初始化组件

#### 1. 初始化 xmanager

```bash
./xmanager -f config/manager_mgr1.cfg init
```

预期输出：
```
---------xmanager config begin--------------
jobname:    xmgr
data:       /path/to/xdata
host:       127.0.0.1
port:       6543
---------xmanager config   end--------------
-------------------------------------
|           pgcastor init success     |
-------------------------------------
```

#### 2. 初始化 capture

```bash
./capture -f config/capture_cap1.cfg init
```

#### 3. 初始化 integrate

```bash
./integrate -f config/integrate_int1.cfg init
```

### 启动服务

```bash
# 按顺序启动：manager -> capture -> integrate
./xmanager -f config/manager_mgr1.cfg start
./capture -f config/capture_cap1.cfg start
./integrate -f config/integrate_int1.cfg start
```

### 验证部署

```bash
# 检查进程
ps aux | grep -E "(xmanager|capture|integrate)" | grep -v grep

# 检查 xmanager 端口
ss -tlnp | grep 6543

# 检查数据目录
ls -la xdata/ capturedata/ integratedata/
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
| `create manager <名称>` | 创建管理器作业 |
| `create capture <名称>` | 创建捕获器作业 |
| `create integrate <名称>` | 创建集成器作业 |
| `create pgreceivelog <名称>` | 创建WAL接收器作业 |

#### 生命周期命令

| 命令 | 描述 |
|------|------|
| `init manager <名称>` | 初始化管理器 |
| `init capture <名称>` | 初始化捕获器 |
| `init integrate <名称>` | 初始化集成器 |
| `start manager <名称>` | 启动管理器 |
| `start capture <名称>` | 启动捕获器 |
| `start integrate <名称>` | 启动集成器 |
| `stop manager <名称>` | 停止管理器 |
| `stop capture <名称>` | 停止捕获器 |
| `stop integrate <名称>` | 停止集成器 |

#### 信息查询命令

| 命令 | 描述 |
|------|------|
| `status manager <名称>` | 显示管理器状态 |
| `status capture <名称>` | 显示捕获器状态 |
| `status integrate <名称>` | 显示集成器状态 |
| `info manager <名称>` | 显示管理器信息 |
| `info capture <名称>` | 显示捕获器信息 |
| `info integrate <名称>` | 显示集成器信息 |
| `watch <类型> <名称>` | 实时监控作业状态 |

#### 其他命令

| 命令 | 描述 |
|------|------|
| `edit <类型> <名称>` | 编辑作业配置 |
| `reload <类型> <名称>` | 重新加载配置 |
| `remove <类型> <名称>` | 删除配置文件 |
| `drop <类型> <名称>` | 删除作业 |
| `alter <类型> <名称>` | 修改进度成员 |
| `help` | 显示帮助 |
| `exit` | 退出 xscsci |

### 直接执行命令

每个组件都可以直接运行：

```bash
# 通用命令格式
./<组件> -f <配置文件> <操作>

# 操作: init, start, stop, status, reload

# 示例
./capture -f config/capture_cap1.cfg init
./capture -f config/capture_cap1.cfg start
./capture -f config/capture_cap1.cfg stop
./capture -f config/capture_cap1.cfg status
```

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

### 主要配置文件

| 文件 | 用途 |
|------|------|
| `etc/capture.cfg` | 捕获器作业配置模板 |
| `etc/integrate.cfg` | 集成器作业配置模板 |
| `etc/xmanager.cfg` | 管理器配置模板 |
| `etc/receivewal.cfg` | WAL接收器配置模板 |

### capture.cfg 参数

| 参数 | 描述 | 示例 |
|------|------|------|
| `jobname` | 作业标识符 | `capture` |
| `url` | 源数据库连接字符串 | `port=5432 dbname=postgres` |
| `data` | 工作目录 | `/path/to/capturedata` |
| `wal_dir` | PostgreSQL WAL目录 | `/path/to/data/pg_wal` |
| `dbtype` | 数据库类型 | `postgres` |
| `dbversion` | PostgreSQL版本 | `12` |
| `catalog_schema` | 同步表所在架构 | `pgcastor` |
| `table` | 要捕获的表 | `*.*`（所有表）|
| `ddl` | 启用DDL同步 | `1`（开启）|

### integrate.cfg 参数

| 参数 | 描述 | 示例 |
|------|------|------|
| `jobname` | 作业标识符 | `integrate` |
| `url` | 目标数据库连接字符串 | `port=5433 dbname=postgres` |
| `data` | 工作目录 | `/path/to/integratedata` |
| `trail_dir` | Trail文件目录 | `/path/to/capturedata` |
| `catalog_schema` | 同步表所在架构 | `pgcastor` |

### xmanager.cfg 参数

| 参数 | 描述 | 默认值 |
|------|------|--------|
| `jobname` | 管理器标识符 | `xmgr` |
| `data` | 工作目录 | - |
| `port` | 网络端口 | `6543` |
| `host` | 绑定地址 | `127.0.0.1` |
| `tcp_keepalive` | 启用TCP保活 | `on` |

---

## 目录结构

```
pgcastor-pro/
├── bin/                    # 可执行文件和源码
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
│   ├── parser/            # SQL/WAL解析
│   ├── utils/             # 工具函数
│   └── ...
├── incl/                  # 头文件
├── interfaces/            # 动态库
│   ├── src/              # 接口源码
│   ├── incl/             # 接口头文件
│   └── lib/              # libpgcastor.so
├── parser/               # 解析器库
├── lib/                  # libcastor_static.a
├── etc/                  # 配置模板
├── build/                # 构建目录
└── work_memory/          # 工作文件
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
3. 检查复制槽：`psql -c "SELECT * FROM pg_replication_slots;"`

### 端口被占用

```bash
# 查找并终止进程
ss -tlnp | grep 6543
kill -9 <PID>
```

---

## 文档

| 文档 | 描述 |
|------|------|
| `AGENTS.md` | 项目架构和代码分析 |
| `compile.md` | 详细编译指南 |
| `run_and_test.md` | 部署和测试指南 |
| `CODE_STYLE.md` | 编码规范 |

---

## 许可证

Copyright (c) 2024-2024, Byte Sync Development Group

---

## 贡献指南

1. Fork 仓库
2. 创建功能分支
3. 进行修改
4. 运行测试
5. 提交 Pull Request

---

*更多信息请参阅 [AGENTS.md](AGENTS.md) 了解架构详情，或 [compile.md](compile.md) 了解详细构建说明。*
