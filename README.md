# PGCastor Pro

**PGCastor Pro** is a PostgreSQL data replication and synchronization toolkit written in C. It provides a comprehensive solution for capturing, transforming, and applying database changes between PostgreSQL instances.

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Features](#features)
- [Quick Start](#quick-start)
- [Build Guide](#build-guide)
- [Deployment Guide](#deployment-guide)
- [Command Reference](#command-reference)
- [Configuration](#configuration)
- [Directory Structure](#directory-structure)
- [Dependencies](#dependencies)
- [Supported PostgreSQL Versions](#supported-postgresql-versions)
- [License](#license)

---

## Project Overview

### Purpose
- Real-time data capture from PostgreSQL WAL (Write-Ahead Log)
- Data transformation and trail file generation
- Data integration/application to target databases
- Centralized management through CLI tool and manager process

### Technology Stack

| Component | Technology |
|-----------|------------|
| Language | C (C99 standard) |
| Build System | CMake 3.10+ |
| Parser Generator | Bison + Flex |
| Database | PostgreSQL 12+ |
| Compression | LZ4 |
| CLI Library | Readline |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           PGCASTOR ECOSYSTEM                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                  │
│  │   xscsci    │◄──►│  xmanager   │◄──►│  capture    │                  │
│  │   (CLI)     │    │  (Manager)  │    │ (Capture)   │                  │
│  └─────────────┘    └─────────────┘    └─────────────┘                  │
│                            │                   │                        │
│                            │                   ▼                        │
│                            │           ┌─────────────┐                  │
│                            │           │ PostgreSQL  │                  │
│                            │           │  (Source)   │                  │
│                            │           └─────────────┘                  │
│                            │                   │                        │
│                            │           WAL/Trail Files                  │
│                            │                   │                        │
│                            │                   ▼                        │
│                            │           ┌─────────────┐                  │
│                            └──────────►│  integrate  │                  │
│                                        │ (Apply)     │                  │
│                                        └─────────────┘                  │
│                                              │                          │
│                                              ▼                          │
│                                        ┌─────────────┐                  │
│                                        │ PostgreSQL  │                  │
│                                        │  (Target)   │                  │
│                                        └─────────────┘                  │
└─────────────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
1. CAPTURE PHASE
   PostgreSQL ──WAL──► capture ──Trail Files──► Storage

2. INTEGRATE PHASE
   Storage ──Trail Files──► integrate ──SQL──► Target PostgreSQL

3. MANAGEMENT PHASE
   xscsci (CLI) ──Commands──► xmanager ──Control──► capture/integrate
```

---

## Features

- **Real-time WAL Capture**: Captures DML/DDL operations from PostgreSQL WAL
- **High Performance**: Multi-threaded architecture with parallel processing
- **Flexible Filtering**: Table include/exclude patterns
- **Big Transaction Handling**: Special handling for large transactions
- **Online Refresh**: Initial data synchronization support
- **DDL Support**: Schema changes replication
- **Conflict Resolution**: Handles data conflicts during integration
- **Centralized Management**: CLI-based job management

---

## Quick Start

### 1. Build the Project

```bash
# Clone repository
git clone <repository-url>
cd pgcastor-pro

# Build
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=debug -DPOSTGRES_INSTALL_DIR=/path/to/pgsql
make -j4
```

### 2. Prepare Environment

```bash
# Set environment variables
export LD_LIBRARY_PATH=/path/to/pgsql/lib:$(pwd)/lib:$(pwd)/interfaces/lib:$LD_LIBRARY_PATH
export PGCASTOR=$(pwd)/install
```

### 3. Start Services

```bash
# Create and start manager
./bin/xscsci/xscsci
xscsci=> create manager mgr1
xscsci=> init manager mgr1
xscsci=> start manager mgr1

# Create and start capture
xscsci=> create capture cap1
xscsci=> init capture cap1
xscsci=> start capture cap1

# Create and start integrate
xscsci=> create integrate int1
xscsci=> init integrate int1
xscsci=> start integrate int1
```

---

## Build Guide

### Prerequisites

| Requirement | Version |
|-------------|---------|
| CMake | 3.10+ |
| GCC/Clang | C99 support |
| PostgreSQL | 12+ |
| Readline | - |
| LZ4 | - |

### Build Steps

```bash
# 1. Create build directory
mkdir -p build && cd build

# 2. Configure with CMake (Debug build)
cmake .. -DCMAKE_BUILD_TYPE=debug -DPOSTGRES_INSTALL_DIR=/path/to/pgsql

# 3. Build
make -j4
```

### Build Targets

| Target | Type | Output |
|--------|------|--------|
| `castor_static` | Static Library | `lib/libcastor_static.a` |
| `pgcastor` | Shared Library | `interfaces/lib/libpgcastor.so` |
| `capture` | Executable | `bin/capture/capture` |
| `integrate` | Executable | `bin/integrate/integrate` |
| `receivepglog` | Executable | `bin/receivepglog/receivepglog` |
| `xmanager` | Executable | `bin/xmanager/xmanager` |
| `xscsci` | Executable | `bin/xscsci/xscsci` |

### Build Types

```bash
# Debug build (development)
cmake .. -DCMAKE_BUILD_TYPE=debug -DPOSTGRES_INSTALL_DIR=/path/to/pgsql

# Release build (production)
cmake .. -DCMAKE_BUILD_TYPE=release -DPOSTGRES_INSTALL_DIR=/path/to/pgsql
```

### Verify Build

```bash
# Check binaries
ls -lh bin/capture/capture bin/integrate/integrate bin/xmanager/xmanager bin/xscsci/xscsci

# Check libraries
ls -lh lib/libcastor_static.a interfaces/lib/libpgcastor.so

# Verify debug symbols
file bin/capture/capture | grep "with debug_info"
```

### Clean Build

```bash
# Clean generated files
make clean_all

# Or remove entire build directory
rm -rf build
```

---

## Deployment Guide

### Environment Setup

```bash
# Add to ~/.bashrc or ~/.zshrc
export PGCASTOR=/path/to/pgcastor-pro/install
export LD_LIBRARY_PATH=/path/to/pgsql/lib:$PGCASTOR/../lib:$PGCASTOR/../interfaces/lib:$LD_LIBRARY_PATH
```

### PostgreSQL Requirements

- Source database: port 5432 (default)
- Target database: port 5433 (or as configured)
- `wal_level = logical` in postgresql.conf
- `max_replication_slots` >= 1

### Initialize Components

#### 1. Initialize xmanager

```bash
./xmanager -f config/manager_mgr1.cfg init
```

Expected output:
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

#### 2. Initialize capture

```bash
./capture -f config/capture_cap1.cfg init
```

#### 3. Initialize integrate

```bash
./integrate -f config/integrate_int1.cfg init
```

### Start Services

```bash
# Start in order: manager -> capture -> integrate
./xmanager -f config/manager_mgr1.cfg start
./capture -f config/capture_cap1.cfg start
./integrate -f config/integrate_int1.cfg start
```

### Verify Deployment

```bash
# Check processes
ps aux | grep -E "(xmanager|capture|integrate)" | grep -v grep

# Check xmanager port
ss -tlnp | grep 6543

# Check data directories
ls -la xdata/ capturedata/ integratedata/
```

### Test Data Synchronization

```bash
# Create test table in source DB
/path/to/pgsql/bin/psql -p 5432 -d postgres -c "CREATE TABLE test_sync(id int);"

# Insert test data
/path/to/pgsql/bin/psql -p 5432 -d postgres -c "INSERT INTO test_sync VALUES (1);"

# Verify in target DB
/path/to/pgsql/bin/psql -p 5433 -d postgres -c "SELECT * FROM test_sync;"
```

---

## Command Reference

### xscsci CLI Commands

The `xscsci` tool provides an interactive CLI for managing PGCastor jobs.

#### Starting xscsci

```bash
./xscsci
```

#### Job Management Commands

| Command | Description |
|---------|-------------|
| `create manager <name>` | Create manager job |
| `create capture <name>` | Create capture job |
| `create integrate <name>` | Create integrate job |
| `create pgreceivelog <name>` | Create WAL receiver job |

#### Lifecycle Commands

| Command | Description |
|---------|-------------|
| `init manager <name>` | Initialize manager |
| `init capture <name>` | Initialize capture |
| `init integrate <name>` | Initialize integrate |
| `start manager <name>` | Start manager |
| `start capture <name>` | Start capture |
| `start integrate <name>` | Start integrate |
| `stop manager <name>` | Stop manager |
| `stop capture <name>` | Stop capture |
| `stop integrate <name>` | Stop integrate |

#### Information Commands

| Command | Description |
|---------|-------------|
| `status manager <name>` | Show manager status |
| `status capture <name>` | Show capture status |
| `status integrate <name>` | Show integrate status |
| `info manager <name>` | Show manager info |
| `info capture <name>` | Show capture info |
| `info integrate <name>` | Show integrate info |
| `watch <type> <name>` | Monitor job status in real-time |

#### Other Commands

| Command | Description |
|---------|-------------|
| `edit <type> <name>` | Edit job configuration |
| `reload <type> <name>` | Reload configuration |
| `remove <type> <name>` | Remove configuration file |
| `drop <type> <name>` | Drop job |
| `alter <type> <name>` | Modify progress members |
| `help` | Show help |
| `exit` | Exit xscsci |

### Direct Executable Commands

Each component can be run directly:

```bash
# Generic command format
./<component> -f <config_file> <operation>

# Operations: init, start, stop, status, reload

# Examples
./capture -f config/capture_cap1.cfg init
./capture -f config/capture_cap1.cfg start
./capture -f config/capture_cap1.cfg stop
./capture -f config/capture_cap1.cfg status
```

---

## Configuration

### Configuration Format

INI-style configuration files with key-value pairs:

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

### Key Configuration Files

| File | Purpose |
|------|---------|
| `etc/capture.cfg` | Capture job configuration template |
| `etc/integrate.cfg` | Integrate job configuration template |
| `etc/xmanager.cfg` | Manager configuration template |
| `etc/receivewal.cfg` | WAL receiver configuration template |

### capture.cfg Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `jobname` | Job identifier | `capture` |
| `url` | Source database connection string | `port=5432 dbname=postgres` |
| `data` | Working directory | `/path/to/capturedata` |
| `wal_dir` | PostgreSQL WAL directory | `/path/to/data/pg_wal` |
| `dbtype` | Database type | `postgres` |
| `dbversion` | PostgreSQL version | `12` |
| `catalog_schema` | Schema for sync tables | `pgcastor` |
| `table` | Tables to capture | `*.*` (all) |
| `ddl` | Enable DDL sync | `1` (on) |

### integrate.cfg Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `jobname` | Job identifier | `integrate` |
| `url` | Target database connection string | `port=5433 dbname=postgres` |
| `data` | Working directory | `/path/to/integratedata` |
| `trail_dir` | Trail files directory | `/path/to/capturedata` |
| `catalog_schema` | Schema for sync tables | `pgcastor` |

### xmanager.cfg Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `jobname` | Manager identifier | `xmgr` |
| `data` | Working directory | - |
| `port` | Network port | `6543` |
| `host` | Bind address | `127.0.0.1` |
| `tcp_keepalive` | Enable TCP keepalive | `on` |

---

## Directory Structure

```
pgcastor-pro/
├── bin/                    # Executables and source
│   ├── capture/           # Capture component
│   ├── integrate/         # Integrate component
│   ├── receivepglog/      # WAL receiver
│   ├── xmanager/          # Manager component
│   └── xscsci/            # CLI tool
├── src/                   # Core library source
│   ├── catalog/           # Catalog management
│   ├── command/           # Command processing
│   ├── elog/              # Logging system
│   ├── net/               # Network layer
│   ├── parser/            # SQL/WAL parsing
│   ├── utils/             # Utilities
│   └── ...
├── incl/                  # Header files
├── interfaces/            # Shared library
│   ├── src/              # Interface source
│   ├── incl/             # Interface headers
│   └── lib/              # libpgcastor.so
├── parser/               # Parser library
├── lib/                  # libcastor_static.a
├── etc/                  # Configuration templates
├── build/                # Build directory
└── work_memory/          # Work files
```

---

## Dependencies

### Required Dependencies

| Dependency | Purpose |
|------------|---------|
| PostgreSQL (libpq) | Database client library |
| Readline | CLI line editing |
| LZ4 | Compression |
| Threads (pthread) | Threading |
| Math (libm) | Mathematical operations |

### Optional Dependencies

| Dependency | Purpose |
|------------|---------|
| Bison | Parser generation (for xscsci) |
| Flex | Lexer generation (for xscsci) |

### Install Dependencies

```bash
# Ubuntu/Debian
sudo apt-get install libreadline-dev liblz4-dev bison flex

# CentOS/RHEL
sudo yum install readline-devel lz4-devel bison flex

# PostgreSQL development packages
sudo apt-get install postgresql-server-dev-12  # Ubuntu
sudo yum install postgresql12-devel            # CentOS
```

---

## Supported PostgreSQL Versions

| Version | Status |
|---------|--------|
| PostgreSQL 12 | Supported |
| PostgreSQL 13 | Planned |
| PostgreSQL 14 | Planned |

---

## Troubleshooting

### Process Won't Start

1. Check logs: `cat xdata/log/*.log`
2. Check PostgreSQL connectivity: `psql -p 5432 -c "SELECT 1;"`
3. Check permissions: `ls -la /path/to/pg_wal/`

### Data Not Synchronizing

1. Verify all processes running: `ps aux | grep -E "(xmanager|capture|integrate)"`
2. Check trail files: `ls -la capturedata/trail/`
3. Check replication slots: `psql -c "SELECT * FROM pg_replication_slots;"`

### Port Already in Use

```bash
# Find and kill process
ss -tlnp | grep 6543
kill -9 <PID>
```

---

## Documentation

| Document | Description |
|----------|-------------|
| `AGENTS.md` | Project architecture and code analysis |
| `compile.md` | Detailed compilation guide |
| `run_and_test.md` | Deployment and testing guide |
| `CODE_STYLE.md` | Coding standards |

---

## License

Copyright (c) 2024-2024, Byte Sync Development Group

---

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests
5. Submit a pull request

---

*For more information, see [AGENTS.md](AGENTS.md) for architecture details or [compile.md](compile.md) for detailed build instructions.*
