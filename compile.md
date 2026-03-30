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

---

## Step 1: PostgreSQL Installation Detection

### Automatic Detection

Before compiling, we need to locate the PostgreSQL installation. Use one of these methods:

#### Method A: Check if pg_config is in PATH
```bash
which pg_config
```
If found, use it to get the installation path:
```bash
pg_config --bindir | sed 's|/bin$||'
```

#### Method B: Common Installation Locations
Check these common paths:
```bash
# Check common PostgreSQL installation directories
ls -d /usr/local/pgsql 2>/dev/null && echo "Found at /usr/local/pgsql"
ls -d /usr/lib/postgresql/*/bin 2>/dev/null && echo "Found system PostgreSQL"
ls -d $HOME/pg/pgsql 2>/dev/null && echo "Found at $HOME/pg/pgsql"
ls -d /opt/postgresql 2>/dev/null && echo "Found at /opt/postgresql"
ls -d /opt/pgsql 2>/dev/null && echo "Found at /opt/pgsql"
```

### Manual Input Required

If PostgreSQL is not found automatically, you will need to provide the installation path manually.

**The system will prompt you:**
```
PostgreSQL installation not found in standard locations.
Please enter the PostgreSQL installation directory path.
This is typically the directory containing 'bin', 'lib', and 'include' subdirectories.
Example: /usr/local/pgsql or /home/youruser/pg/pgsql

PostgreSQL Installation Path: _
```

### Verify PostgreSQL Installation

Once you have the path (either detected or provided), verify it:

```bash
# Set the PostgreSQL installation path
export PG_INSTALL_DIR=/path/to/your/pgsql

# Verify the installation
ls -la $PG_INSTALL_DIR/bin/pg_config
ls -la $PG_INSTALL_DIR/lib/libpq.*
ls -la $PG_INSTALL_DIR/include/postgres.h

# Check PostgreSQL version
$PG_INSTALL_DIR/bin/pg_config --version
```

**Expected Output:**
```
PostgreSQL 12.x
```

---

## Step 2: Verify All Dependencies

### Check CMake and Make
```bash
cmake --version
make --version
```

**Required:** CMake 3.10 or higher

### Check PostgreSQL Libraries
```bash
# Using detected path
ls $PG_INSTALL_DIR/lib/libpq.*

# Check for server headers
ls $PG_INSTALL_DIR/include/server/postgres.h
```

### Check Bison and Flex (Optional)
```bash
which bison flex
bison --version
flex --version
```

### Check System Libraries
```bash
ldconfig -p | grep -E "libreadline|liblz4"
```

---

## Step 3: Navigate to Project Root

```bash
cd /home/liu/pgcastor/pgcastor-pro
```

---

## Step 4: Create Build Directory

```bash
mkdir -p build
cd build
```

---

## Step 5: Configure with CMake

### Automatic Configuration (Recommended)

If PostgreSQL was detected automatically:
```bash
cmake .. -DCMAKE_BUILD_TYPE=debug -DPOSTGRES_INSTALL_DIR=$PG_INSTALL_DIR
```

### Manual Configuration

If you need to specify the path manually:
```bash
# Replace /path/to/pgsql with your actual PostgreSQL installation path
cmake .. -DCMAKE_BUILD_TYPE=debug -DPOSTGRES_INSTALL_DIR=/path/to/pgsql
```

### Expected Output

```
-- The C compiler identification is GNU X.X.X
-- Detecting C compiler ABI info
-- Detecting C compiler ABI info - done
-- Check for working C compiler: /usr/bin/cc - skipped
-- Detecting C compile features
-- Detecting C compile features - done
-- Performing Test CMAKE_HAVE_LIBC_PTHREAD
-- Performing Test CMAKE_HAVE_LIBC_PTHREAD - Success
-- Found Threads: TRUE
-- Configuring done (0.5s)
-- Generating done (0.2s)
-- Build files have been written to: /home/liu/pgcastor/pgcastor-pro/build
```

### If Configuration Fails

**Error:** `PostgreSQL not found. Please specify POSTGRES_INSTALL_DIR.`

**Interactive Prompt Will Appear:**
```
===========================================
ERROR: PostgreSQL installation not found!
===========================================

CMake could not locate PostgreSQL installation.

Please check:
1. Is PostgreSQL installed on this system?
2. Do you have the development packages installed?
   - On Ubuntu/Debian: sudo apt-get install postgresql-server-dev-12
   - On CentOS/RHEL: sudo yum install postgresql12-devel

3. What is your PostgreSQL installation directory?
   (This is the directory containing bin/, lib/, include/ subdirectories)

Please enter the full path to your PostgreSQL installation directory: _
```

**To fix manually:**
```bash
# Try finding PostgreSQL
find /usr -name pg_config -type f 2>/dev/null
find /opt -name pg_config -type f 2>/dev/null
find $HOME -name pg_config -type f 2>/dev/null

# Once found, extract the base directory
# If pg_config is at /home/user/pg/pgsql/bin/pg_config
# Then use: /home/user/pg/pgsql

cmake .. -DCMAKE_BUILD_TYPE=debug -DPOSTGRES_INSTALL_DIR=/home/user/pg/pgsql
```

---

## Step 6: Compile the Project

```bash
make -j4
```

### Expected Output

```
[  0%] Built target generate_parser_files
[  1%] Building C object CMakeFiles/castor_static.dir/src/...
...
[ 97%] Built target castor_static
[ 98%] Built target integrate
[ 98%] Built target capture
[ 99%] Built target xsynch
[ 99%] Built target receivepglog
[ 99%] Built target xmanager
[100%] Built target xscsci
```

### Compilation Progress Indicators

- **generate_parser_files**: Bison/Flex parser generation (first step)
- **castor_static**: Core static library (largest compilation unit)
- **xsynch**: Shared interface library
- **capture/integrate/xmanager/xscsci**: Final executables

---

## Step 7: Verify Binaries

After successful compilation, verify that all binaries were created:

```bash
ls -lh bin/capture/capture bin/integrate/integrate bin/xmanager/xmanager bin/xscsci/xscsci bin/receivepglog/receivepglog
```

**Expected Output:**
```
-rwxrwxr-x 1 user user 7.5M [date] bin/capture/capture
-rwxrwxr-x 1 user user 7.5M [date] bin/integrate/integrate
-rwxrwxr-x 1 user user 7.6M [date] bin/receivepglog/receivepglog
-rwxrwxr-x 1 user user 7.5M [date] bin/xmanager/xmanager
-rwxrwxr-x 1 user user 285K [date] bin/xscsci/xscsci
```

### Verify Libraries

```bash
ls -lh lib/libcastor_static.a interfaces/lib/libxsynch.so
```

**Expected Output:**
```
-rw-rw-r-- 1 user user  16M [date] lib/libcastor_static.a
-rwxrwxr-x 1 user user 219K [date] interfaces/lib/libxsynch.so
```

### Verify Debug Build

```bash
# Check build type
grep "CMAKE_BUILD_TYPE" CMakeCache.txt

# Verify debug symbols in binary
file bin/capture/capture | grep "with debug_info"
```

**Expected Output:**
```
CMAKE_BUILD_TYPE:STRING=debug
bin/capture/capture: ELF 64-bit LSB pie executable, x86-64, ..., with debug_info, not stripped
```

---

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

---

## Clean Build

To clean all build artifacts:

```bash
cd /home/liu/pgcastor/pgcastor-pro/build
make clean_all

# Or remove the entire build directory
rm -rf build
mkdir build && cd build
```

---

## Troubleshooting

### PostgreSQL Not Found

**Symptom:**
```
CMake Error at CMakeLists.txt:32 (message):
  PostgreSQL not found. Please specify POSTGRES_INSTALL_DIR.
```

**Solution:**

1. **Find PostgreSQL installation:**
   ```bash
   # Search for pg_config
   find / -name pg_config -type f 2>/dev/null
   
   # Or check common locations
   ls -la /usr/local/pgsql/bin/pg_config
   ls -la /usr/lib/postgresql/*/bin/pg_config
   ls -la $HOME/pg/pgsql/bin/pg_config
   ```

2. **Verify installation:**
   ```bash
   # Set your PostgreSQL path
   export PG_PATH=/path/to/found/pgsql
   
   # Verify structure
   ls $PG_PATH/bin/pg_config
   ls $PG_PATH/lib/libpq.*
   ls $PG_PATH/include/postgres.h
   ```

3. **Configure CMake:**
   ```bash
   cmake .. -DCMAKE_BUILD_TYPE=debug -DPOSTGRES_INSTALL_DIR=$PG_PATH
   ```

### Missing Dependencies

**Error:** `Readline library not found.` or `LZ4 library not found.`

**Solution:** Install the missing dependencies:

```bash
# Ubuntu/Debian
sudo apt-get install libreadline-dev liblz4-dev

# CentOS/RHEL
sudo yum install readline-devel lz4-devel

# Fedora
sudo dnf install readline-devel lz4-devel

# Arch Linux
sudo pacman -S readline lz4
```

### Bison/Flex Not Found

**Error:** Parser files not generated.

**Solution:** Install Bison and Flex:

```bash
# Ubuntu/Debian
sudo apt-get install bison flex

# CentOS/RHEL
sudo yum install bison flex

# Fedora
sudo dnf install bison flex

# Arch Linux
sudo pacman -S bison flex
```

### PostgreSQL Development Headers Missing

**Error:** `postgres.h not found`

**Solution:** Install PostgreSQL development packages:

```bash
# Ubuntu/Debian (for PostgreSQL 12)
sudo apt-get install postgresql-server-dev-12

# CentOS/RHEL (for PostgreSQL 12)
sudo yum install postgresql12-devel

# If using custom PostgreSQL installation, ensure server headers are present
ls $PG_INSTALL_DIR/include/server/postgres.h
```

### Compilation Errors

If you encounter compilation errors:

1. **Check CMake version:**
   ```bash
   cmake --version  # Must be 3.10 or higher
   ```

2. **Verify compiler:**
   ```bash
   gcc --version  # Or clang --version
   # Must support C99 standard
   ```

3. **Check PostgreSQL headers:**
   ```bash
   ls $PG_INSTALL_DIR/include/server/
   ```

4. **Clean and rebuild:**
   ```bash
   rm -rf build
   mkdir build && cd build
   cmake .. -DCMAKE_BUILD_TYPE=debug -DPOSTGRES_INSTALL_DIR=$PG_INSTALL_DIR
   make -j4
   ```

---

## Build Configuration Options

### Build Types

- **Debug:** `cmake .. -DCMAKE_BUILD_TYPE=debug` (default for development)
- **Release:** `cmake .. -DCMAKE_BUILD_TYPE=release` (optimized build)
- **RelWithDebInfo:** `cmake .. -DCMAKE_BUILD_TYPE=relwithdebinfo`
- **MinSizeRel:** `cmake .. -DCMAKE_BUILD_TYPE=minsizerel`

### PostgreSQL Path Configuration

Always specify the PostgreSQL installation directory:

```bash
-DPOSTGRES_INSTALL_DIR=/path/to/postgresql
```

### Example Configurations

**Debug Build (Development):**
```bash
cmake .. \
  -DCMAKE_BUILD_TYPE=debug \
  -DPOSTGRES_INSTALL_DIR=/home/user/pg/pgsql
```

**Release Build (Production):**
```bash
cmake .. \
  -DCMAKE_BUILD_TYPE=release \
  -DPOSTGRES_INSTALL_DIR=/usr/local/pgsql
```

---

## Post-Compilation Steps

After successful compilation:

### 1. Prepare Installation Directory

```bash
mkdir -p install/config/sample
cp bin/capture/capture bin/integrate/integrate bin/xmanager/xmanager bin/xscsci/xscsci bin/receivepglog/receivepglog install/
```

### 2. Copy Configuration Templates

```bash
cp etc/capture.cfg install/config/sample/capture.cfg.sample
cp etc/integrate.cfg install/config/sample/integrate.cfg.sample
cp etc/xmanager.cfg install/config/sample/manager.cfg.sample
```

### 3. Set Environment Variables

Add to your `~/.bashrc` or `~/.zshrc`:

```bash
# pgcastor-pro environment
export LD_LIBRARY_PATH=$PG_INSTALL_DIR/lib:/home/liu/pgcastor/pgcastor-pro/lib:/home/liu/pgcastor/pgcastor-pro/interfaces/lib:$LD_LIBRARY_PATH
export XSYNCH=/home/liu/pgcastor/pgcastor-pro/install
```

Apply immediately:
```bash
source ~/.bashrc  # or source ~/.zshrc
```

---

## Quick Reference: Interactive PostgreSQL Path Detection

When you run the compilation instructions, the system will:

1. **Try automatic detection:**
   - Check `which pg_config`
   - Search common installation directories
   - Look for environment variables (`PG_INSTALL_DIR`, `POSTGRES_HOME`)

2. **Prompt if not found:**
   ```
   ╔════════════════════════════════════════════════════════════╗
   ║     PostgreSQL Installation Path Required                  ║
   ╠════════════════════════════════════════════════════════════╣
   ║ CMake could not locate PostgreSQL automatically.           ║
   ║                                                             ║
   ║ Please enter the full path to your PostgreSQL installation ║
   ║ (the directory containing bin/, lib/, include/):           ║
   ║                                                             ║
   ║ Examples:                                                   ║
   ║   /usr/local/pgsql                                          ║
   ║   /home/yourname/pg/pgsql                                   ║
   ║   /usr/lib/postgresql/12                                    ║
   ╚════════════════════════════════════════════════════════════╝
   
   PostgreSQL Installation Path: 
   ```

3. **Validate the input:**
   - Check if directory exists
   - Verify `bin/pg_config` exists
   - Verify `lib/libpq.*` exists
   - Verify `include/server/postgres.h` exists

4. **Proceed with compilation** if validation succeeds

---

## Additional Resources

- Project README: `/home/liu/pgcastor/pgcastor-pro/README.md`
- Project Architecture: `/home/liu/pgcastor/pgcastor-pro/AGENTS.md`
- Code Style Guide: `/home/liu/pgcastor/pgcastor-pro/CODE_STYLE.md`
- Configuration Templates: `/home/liu/pgcastor/pgcastor-pro/etc/`
- Run and Test Guide: `/home/liu/pgcastor/pgcastor-pro/run_and_test.md`

---

## Verification Checklist

Before considering the compilation complete:

- [ ] PostgreSQL installation detected or provided
- [ ] CMake configuration succeeded without errors
- [ ] All binaries created in `bin/` directories
- [ ] Libraries created: `lib/libcastor_static.a`, `interfaces/lib/libxsynch.so`
- [ ] Debug symbols present in binaries (`file` command shows "with debug_info")
- [ ] Environment variables set (`LD_LIBRARY_PATH`, `XSYNCH`)
- [ ] Installation directory prepared with binaries and configs

---

*This skill was updated on April 1, 2026 with interactive PostgreSQL path detection*
