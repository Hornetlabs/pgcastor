# pgcastor-pro

## 项目概述
pgcastor-pro 是一个为 PostgreSQL 提供的工具集项目，包括 capture、integrate、receivepglog、xmanager 和 xscsci 组件。

## 依赖项

### 必需依赖
- **PostgreSQL**：强大的开源关系型数据库系统
- **Readline**：用于行输入和编辑的库
- **LZ4**：无损压缩算法
- **Threads**：系统线程库
- **Math**：系统数学库

### 可选依赖
- **Bison**：用于语法分析的解析器生成器
- **Flex**：词法分析器生成器

## 编译指南

### 先决条件
1. 确保系统上安装了所有依赖项
2. 安装 CMake（2.8 或更高版本）

### 编译步骤
1. **克隆仓库**
   ```bash
   git clone <仓库地址>
   cd pgcastor-pro
   ```

2. **使用 CMake 配置**
   ```bash
   mkdir -p build
   cd build
   cmake .. -DPOSTGRES_INSTALL_DIR=/path/to/postgresql
   ```
   如果 PostgreSQL 安装在标准位置，可以省略安装目录：
   ```bash
   cmake ..
   ```

3. **构建项目**
   ```bash
   make -j4
   ```
   `-j4` 选项启用 4 线程并行编译。

   要以 debug 模式构建项目：
   ```bash
   cmake .. -DCMAKE_BUILD_TYPE=debug -DPOSTGRES_INSTALL_DIR=/path/to/postgresql
   make -j4
   ```

4. **运行工具**
   可执行文件将生成在 `bin` 目录中：
   - `bin/capture/capture`
   - `bin/integrate/integrate`
   - `bin/receivepglog/receivepglog`
   - `bin/xmanager/xmanager`
   - `bin/xscsci/xscsci`

### 清理构建
要清理构建文件：
```bash
make clean_all
```

## 目录结构
- `bin/`：包含可执行文件
- `src/`：包含源代码
- `incl/`：包含头文件
- `interfaces/`：包含接口库
- `parser/`：包含解析器代码
- `test/`：包含测试文件
- `configsh/`：包含配置脚本
