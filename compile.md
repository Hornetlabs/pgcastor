# pgcastor-pro Compilation Guide

## Overview

This skill provides instructions for compiling the pgcastor-pro (XSynch) PostgreSQL replication toolkit in debug mode.

## Prerequisites

### Required Dependencies
- **CMake** (version 3.10 or higher)
- **GNU Make**
- **PostgreSQL** (version 12+) with development libraries
- **Readline** library
- **LZ4** compression library
- **Bison** (parser generator, optional but recommended)
- **Flex** (lexer generator, optional but recommended)

### PostgreSQL Installation
Ensure PostgreSQL is installed at a known location. Default path used in this guide:
```
/home/liu/pg/pgsql
```

## Compilation Steps

### 1. Verify Dependencies

Check that all required tools are installed:

```bash
# Check CMake version
cmake --version

# Check Make version
make --version

# Check PostgreSQL libraries
ls /home/liu/pg/pgsql/lib/libpq.*

# Check Bison and Flex (optional)
which bison flex

# Check system libraries
ldconfig -p | grep -E "libreadline|liblz4"
```

### 2. Navigate to Project Root

```bash
cd /home/liu/pgcastor/pgcastor-pro
```

### 3. Create Build Directory

```bash
mkdir -p build
cd build
```

### 4. Configure with CMake (Debug Mode)

```bash
cmake .. -DCMAKE_BUILD_TYPE=debug -DPOSTGRES_INSTALL_DIR=/home/liu/pg/pgsql
```

**Expected Output:**
```
-- Configuring done (0.0s)
-- Generating done (0.2s)
-- Build files have been written to: /home/liu/pgcastor/pgcastor-pro/build
```

### 5. Compile the Project

```bash
make -j4
```

**Expected Output:**
```
[  0%] Built target generate_parser_files
[ 97%] Built target castor_static
[ 98%] Built target integrate
[ 98%] Built target capture
[ 99%] Built target xsynch
[ 99%] Built target receivepglog
[ 99%] Built target xmanager
[100%] Built target xscsci
```

### 6. Verify Binaries

After successful compilation, verify that all binaries were created:

```bash
ls -la bin/capture/capture bin/integrate/integrate bin/xmanager/xmanager bin/xscsci/xscsci
```

**Expected Output:**
```
-rwxrwxr-x 1 liu liu 7861000 [date] bin/capture/capture
-rwxrwxr-x 1 liu liu 7860304 [date] bin/integrate/integrate
-rwxrwxr-x 1 liu liu 7860224 [date] bin/xmanager/xmanager
-rwxrwxr-x 1 liu liu  291496 [date] bin/xscsci/xscsci
```

## Build Targets

The project builds the following components:

| Target | Type | Output Location | Description |
|--------|------|----------------|-------------|
| `castor_static` | Static Library | `lib/libcastor_static.a` | Core library |
| `xsynch` | Shared Library | `interfaces/lib/libxsynch.so` | Interface library |
| `capture` | Executable | `bin/capture/capture` | Capture process |
| `integrate` | Executable | `bin/integrate/integrate` | Integrate process |
| `xmanager` | Executable | `bin/xmanager/xmanager` | Manager process |
| `xscsci` | Executable | `bin/xscsci/xscsci` | CLI tool |
| `receivepglog` | Executable | `bin/receivepglog/receivepglog` | WAL receiver |

## Clean Build

To clean all build artifacts:

```bash
cd /home/liu/pgcastor/pgcastor-pro/build
make clean_all
```

## Troubleshooting

### CMake Configuration Fails

**Error:** `PostgreSQL not found. Please specify POSTGRES_INSTALL_DIR.`

**Solution:** Ensure the correct PostgreSQL installation path is specified:
```bash
cmake .. -DCMAKE_BUILD_TYPE=debug -DPOSTGRES_INSTALL_DIR=/correct/path/to/pgsql
```

### Missing Dependencies

**Error:** `Readline library not found.` or `LZ4 library not found.`

**Solution:** Install the missing dependencies:
```bash
# Ubuntu/Debian
sudo apt-get install libreadline-dev liblz4-dev

# CentOS/RHEL
sudo yum install readline-devel lz4-devel
```

### Bison/Flex Not Found

**Error:** Parser files not generated.

**Solution:** Install Bison and Flex:
```bash
# Ubuntu/Debian
sudo apt-get install bison flex

# CentOS/RHEL
sudo yum install bison flex
```

### Compilation Errors

If you encounter compilation errors:
1. Check that you're using CMake 3.10 or higher
2. Ensure PostgreSQL headers are accessible
3. Verify all library paths are correct
4. Check compiler version compatibility (C99 standard required)

## Build Configuration Options

### Build Types

- **Debug:** `cmake .. -DCMAKE_BUILD_TYPE=debug`
- **Release:** `cmake .. -DCMAKE_BUILD_TYPE=release`
- **Default:** No build type specified (uses default compiler flags)

### PostgreSQL Path

Always specify the PostgreSQL installation directory:
```bash
-DPOSTGRES_INSTALL_DIR=/path/to/postgresql
```

## Post-Compilation Steps

After successful compilation:

1. **Copy binaries to install directory:**
   ```bash
   mkdir -p install
   cp bin/capture/capture bin/integrate/integrate bin/xmanager/xmanager bin/xscsci/xscsci install/
   ```

2. **Create configuration directories:**
   ```bash
   mkdir -p install/config/sample
   ```

3. **Set environment variables:**
   ```bash
   export LD_LIBRARY_PATH=/home/liu/pg/pgsql/lib:/home/liu/pgcastor/pgcastor-pro/lib:/home/liu/pgcastor/pgcastor-pro/interfaces/lib
   export XSYNCH=/home/liu/pgcastor/pgcastor-pro/install
   ```

## Additional Resources

- Project README: `/home/liu/pgcastor/pgcastor-pro/README.md`
- Code Style Guide: `/home/liu/pgcastor/pgcastor-pro/CODE_STYLE.md`
- Configuration Templates: `/home/liu/pgcastor/pgcastor-pro/etc/`

## Verification

To verify a successful debug build:

```bash
# Check build type in compile flags
cd build
grep -r "CMAKE_BUILD_TYPE" CMakeCache.txt

# Verify debug symbols in binary
file bin/capture/capture | grep "with debug_info"
```

---

*This skill was generated from successful compilation on April 1, 2026*
