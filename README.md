# pgcastor-pro

## Project Overview
pgcastor-pro is a project that provides a set of tools for PostgreSQL, including capture, integrate, receivepglog, xmanager, and xscsci components.

## Dependencies

### Required Dependencies
- **PostgreSQL**: A powerful open-source relational database system
- **Readline**: A library for line input and editing
- **LZ4**: A lossless compression algorithm
- **Threads**: System thread library
- **Math**: System math library

### Optional Dependencies
- **Bison**: A parser generator for syntax analysis
- **Flex**: A lexical analyzer generator

## Compilation Guide

### Prerequisites
1. Ensure all dependencies are installed on your system
2. Install CMake (version 2.8 or higher)

### Compilation Steps
1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd pgcastor-pro
   ```

2. **Configure with CMake**
   ```bash
   mkdir -p build
   cd build
   cmake .. -DPOSTGRES_INSTALL_DIR=/path/to/postgresql
   ```
   If PostgreSQL installed in standard locations, you can omit the installation directories:
   ```bash
   cmake ..
   ```

3. **Build the project**
   ```bash
   make -j4
   ```
   The `-j4` option enables parallel compilation with 4 threads.

   To build the project in debug mode:
   ```bash
   cmake .. -DCMAKE_BUILD_TYPE=debug -DPOSTGRES_INSTALL_DIR=/path/to/postgresql
   make -j4
   ```

4. **Run the tools**
   The executable files will be generated in the `bin` directory:
   - `bin/capture/capture`
   - `bin/integrate/integrate`
   - `bin/receivepglog/receivepglog`
   - `bin/xmanager/xmanager`
   - `bin/xscsci/xscsci`

### Clean Build
To clean the build files:
```bash
make clean_all
```

## Directory Structure
- `bin/`: Contains executable files
- `src/`: Contains source code
- `incl/`: Contains header files
- `interfaces/`: Contains interface libraries
- `parser/`: Contains parser code
- `test/`: Contains test files
- `configsh/`: Contains configuration scripts
