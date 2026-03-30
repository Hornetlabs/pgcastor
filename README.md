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
                            Control Flow
                    ┌────────────────────┐
                    ▼                    │
┌──────────┐    ┌──────────┐    ┌────────┴────┐    ┌──────────┐
│  xscsci  │◄──►│ xmanager │───►│   capture   │───►│  Trail   │
│  (CLI)   │    │ (Manager)│    │  (Capture)  │    │  Files   │
└──────────┘    └──────────┘    └─────────────┘    └──────────┘
                                     ▲                 │
                                     │ WAL             │ Read Trail
                                     │                 ▼
                                ┌──────────┐      ┌─────────────┐
                                │PostgreSQL│      │  integrate  │
                                │ (Source) │      │ (Integrate) │
                                └──────────┘      └────┬────────┘
                                                       │ Apply SQL
                                                       ▼
                                                  ┌──────────┐
                                                  │PostgreSQL│
                                                  │ (Target) │
                                                  └──────────┘
```

**Architecture Components:**

| Component | Role | Responsibility |
|-----------|------|----------------|
| xscsci | Command Line Interface | User interaction entry point, sends management commands |
| xmanager | Central Manager | Coordinates capture and integrate processes |
| capture | Data Capture | Captures changes from source database WAL, generates Trail files |
| integrate | Data Integration | Reads Trail files, applies changes to target database |

### Data Flow

```
1. Capture Phase
   PostgreSQL (Source) ──WAL──► capture ──Trail Files──► Storage

2. Integrate Phase
   Storage ──Trail Files──► integrate ──SQL──► PostgreSQL (Target)

3. Management Phase
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

## Parser Library Support

### Data Types

#### Integer Types

| Name | Aliases | Storage | Description | Range | Output Format |
|------|---------|---------|-------------|-------|---------------|
| smallint | int2 | 2 bytes | Signed 2-byte integer | -2^15 ~ 2^15-1 | Integer |
| smallserial | serial2 | 2 bytes | Auto-increment 2-byte integer | 1 ~ 2^15-1 | Integer |
| integer | int, int4 | 4 bytes | Signed 4-byte integer | -2^31 ~ 2^31-1 | Integer |
| serial | serial4 | 4 bytes | Auto-increment 4-byte integer | 1 ~ 2^31-1 | Integer |
| bigint | int8 | 8 bytes | Signed 8-byte integer | -2^63 ~ 2^63-1 | Integer |
| bigserial | serial8 | 8 bytes | Auto-increment 8-byte integer | 1 ~ 2^63-1 | Integer |

#### Floating-Point Types

| Name | Aliases | Storage | Description | Range | Output Format |
|------|---------|---------|-------------|-------|---------------|
| double precision | float8 | 8 bytes | Double precision floating-point | 15 significant digits | Decimal |
| real | float4 | 4 bytes | Single precision floating-point | 6 significant digits | Decimal |
| numeric(p, s) | decimal(p, s) | Variable | Exact numeric | 131072 digits before decimal; 16383 after | (p-s).s |
| money | - | 8 bytes | Currency amount | Same as numeric, default 2 decimal places | xx.xx |

#### Character Types

| Name | Aliases | Storage | Description | Output Format |
|------|---------|---------|-------------|---------------|
| character(n) | char(n) | n | Fixed-length string | Single character |
| character varying(n) | varchar(n) | n | Variable-length string | String |
| text | - | Unlimited | Variable-length string | String |

#### Binary Types

| Name | Aliases | Storage | Description | Output Format |
|------|---------|---------|-------------|---------------|
| bit(n) | - | n | Fixed-length bit string | Binary string |
| bit varying(n) | varbit(n) | n | Variable-length bit string | Binary string |
| bytea | - | Unlimited | Binary data | Hexadecimal string |

#### Date/Time Types

| Name | Aliases | Storage | Description | Output Format |
|------|---------|---------|-------------|---------------|
| date | - | 4 bytes | Calendar date | YYYY-MM-DD |
| time(p) [without time zone] | - | 8 bytes | Time of day (no timezone) | hh:mi:ss |
| time(p) with time zone | timetz | 12 bytes | Time of day with timezone | hh:mi:ss+zone |
| timestamp(p) [without time zone] | - | 8 bytes | Date and time (no timezone) | YYYY-MM-DD hh:mi:ss |
| timestamp(p) with time zone | timestamptz | 8 bytes | Date and time with timezone | YYYY-MM-DD hh:mi:ss+zone |

> **Note**: `time` and `timestamp` default to `without time zone`. `timestamptz` is converted to standard value in WAL log, displayed according to session timezone.

#### Boolean Type

| Name | Aliases | Storage | Description | Output Format |
|------|---------|---------|-------------|---------------|
| boolean | bool | 1 byte | Logical boolean (true/false) | t/f |

#### Network Address Types

| Name | Storage | Description | Output Format |
|------|---------|-------------|---------------|
| cidr | 7 or 19 bytes | IPv4 or IPv6 network address | 192.168.100.128/25 |
| inet | 7 or 19 bytes | IPv4 or IPv6 host address | 192.168.1.1/24 |
| macaddr | 6 bytes | MAC address | 08:00:2b:01:02:03 |
| macaddr8 | 8 bytes | MAC address (EUI-64 format) | 08:00:2b:01:02:03:04:05 |

#### Geometric Types

| Name | Storage | Description | Output Format |
|------|---------|-------------|---------------|
| point | 16 bytes | Point on a plane (x,y) | (x, y) |
| line | 32 bytes | Infinite line {A,B,C} | {A, B, C} |
| lseg | 32 bytes | Finite line segment | [(x1, y1), (x2, y2)] |
| box | 32 bytes | Rectangular box | ((x1, y1), (x2, y2)) |
| path | 16+16n bytes | Closed/open path | [(x1, y1), ..., (xn, yn)] |
| polygon | 40+16n bytes | Polygon | ((x1, y1), ..., (xn, yn)) |
| circle | 24 bytes | Circle | <(x, y), r> |

#### Other Types

| Category | Supported Types | Notes |
|----------|-----------------|-------|
| UUID | uuid | Universally unique identifier, 16 bytes |
| XML | xml | XML data, variable length |
| Text Search | tsvector, tsquery | Text search document and query |
| JSON | json, jsonb | JSON text type and binary storage |
| Array | array | Only PostgreSQL native type arrays supported |
| Range | range | User-defined and built-in range types |
| Domain | domain | User-defined type based on another underlying type |
| pg_lsn | pg_lsn | Log sequence number, 8 bytes |

#### Type Support Limitations

| Type | Status | Notes |
|------|--------|-------|
| Composite Types | Issues exist, not supported | User-defined composite types |
| Object Identifier Types | Only oid supported | Other OID types not supported |
| Pseudo Types | Not supported | any, anyelement, cstring, etc. |
| Large Object Types | Not supported | Large Object stored in pg_largeobject |
| jsonpath | Not supported | JSON path expression |

---

### DML

PGCastor supports the following DML operations:

| Operation | Status | Description |
|-----------|--------|-------------|
| INSERT | ✓ Supported | Insert data |
| UPDATE | ✓ Supported | Update data |
| DELETE | ✓ Supported | Delete data |
| Multi-INSERT | ✓ Supported | Multi-row insert |

---

### DDL

PGCastor supports the following DDL operations:

#### Table Operations

| Operation | Sub-features | Status |
|-----------|--------------|--------|
| CREATE TABLE | Primary key, foreign key, unique, check, not null constraints | ✓ Supported |
| | Column defaults (simple expression defaults, regular defaults) | ✓ Supported |
| | Table inheritance (single table, multi-table inheritance) | ✓ Supported |
| | Partitioning (hash, range, list, nested sub-partitions, expression partition keys) | ✓ Supported |
| | Temporary tables | ✓ Supported |
| | UNLOGGED tables | ✓ Supported |
| DROP TABLE | | ✓ Supported |
| ALTER TABLE | Rename table | ✓ Supported |
| | Add/drop constraints (primary key, foreign key, unique, check, not null) | ✓ Supported |
| | Add/drop columns | ✓ Supported |
| | Add/drop column defaults | ✓ Supported |
| | Change column type | ✓ Supported |
| | Change table schema | ✓ Supported |
| | ATTACH/DETACH partition | ✓ Supported |
| TRUNCATE TABLE | | ✓ Supported |

#### Type Operations

| Operation | Sub-features | Status |
|-----------|--------------|--------|
| CREATE TYPE | Composite type | ✓ Supported |
| | Range type | ✓ Supported |
| | Enum type | ✓ Supported |
| | Domain type | ✓ Supported |
| DROP TYPE | | ✓ Supported |

#### Index Operations

| Operation | Sub-features | Status |
|-----------|--------------|--------|
| CREATE INDEX | btree/brin/gin/gist/hash/spgist, multi-column indexes supported | ✓ Supported |
| ALTER INDEX | | Not supported |
| DROP INDEX | | ✓ Supported |
| REINDEX | | ✓ Supported |

#### Schema Operations

| Operation | Status |
|-----------|--------|
| CREATE SCHEMA | ✓ Supported |
| DROP SCHEMA | ✓ Supported |

---

### DDL SQL Examples

The following are PostgreSQL standard SQL syntax examples for each DDL operation:

#### CREATE TABLE Examples

```sql
-- Create table with constraints
CREATE TABLE employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE,
    salary NUMERIC(10, 2) CHECK (salary > 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create partitioned table (range partition)
CREATE TABLE orders (
    order_id BIGINT,
    order_date DATE,
    customer_id INTEGER,
    amount NUMERIC
) PARTITION BY RANGE (order_date);

-- Create partition
CREATE TABLE orders_2024 PARTITION OF orders
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

-- Create hash partitioned table
CREATE TABLE users (
    user_id BIGINT,
    username VARCHAR(50),
    email VARCHAR(100)
) PARTITION BY HASH (user_id);

-- Create list partitioned table
CREATE TABLE sales_by_region (
    id SERIAL,
    region TEXT,
    amount NUMERIC
) PARTITION BY LIST (region);

-- Create temporary table
CREATE TEMP TABLE temp_data (
    id INTEGER,
    value TEXT
);

-- Create UNLOGGED table
CREATE UNLOGGED TABLE session_data (
    session_id UUID PRIMARY KEY,
    data JSONB
);

-- Create inherited table
CREATE TABLE cities (
    name TEXT,
    population BIGINT,
    elevation INTEGER
);

CREATE TABLE capitals (
    state CHAR(2)
) INHERITS (cities);
```

#### DROP TABLE Examples

```sql
-- Drop table
DROP TABLE employees;

-- Drop if exists
DROP TABLE IF EXISTS employees;

-- Cascade drop (drop dependent objects)
DROP TABLE departments CASCADE;
```

#### ALTER TABLE Examples

```sql
-- Rename table
ALTER TABLE employees RENAME TO staff;

-- Add column
ALTER TABLE employees ADD COLUMN phone VARCHAR(20);

-- Drop column
ALTER TABLE employees DROP COLUMN phone;

-- Add constraint
ALTER TABLE employees ADD CONSTRAINT fk_department 
    FOREIGN KEY (department_id) REFERENCES departments(id);

-- Drop constraint
ALTER TABLE employees DROP CONSTRAINT fk_department;

-- Set default value
ALTER TABLE employees ALTER COLUMN status SET DEFAULT 'active';

-- Drop default value
ALTER TABLE employees ALTER COLUMN status DROP DEFAULT;

-- Change column type
ALTER TABLE employees ALTER COLUMN salary TYPE NUMERIC(12, 2);

-- Change table schema
ALTER TABLE employees SET SCHEMA hr;

-- Attach partition
ALTER TABLE orders ATTACH PARTITION orders_2024
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

-- Detach partition
ALTER TABLE orders DETACH PARTITION orders_2024;
```

#### TRUNCATE TABLE Examples

```sql
-- Truncate table
TRUNCATE TABLE employees;

-- Cascade truncate (truncate tables with foreign key references)
TRUNCATE TABLE departments CASCADE;

-- Restart sequences
TRUNCATE TABLE employees RESTART IDENTITY;
```

#### CREATE TYPE Examples

```sql
-- Create composite type
CREATE TYPE address AS (
    street VARCHAR(100),
    city VARCHAR(50),
    zip_code VARCHAR(10)
);

-- Create enum type
CREATE TYPE status AS ENUM ('active', 'inactive', 'pending');

-- Create range type
CREATE TYPE float8range AS RANGE (
    subtype = float8,
    subtype_diff = float8mi
);

-- Create domain type
CREATE DOMAIN positive_integer AS INTEGER CHECK (VALUE > 0);
```

#### DROP TYPE Examples

```sql
-- Drop type
DROP TYPE address;

-- Drop if exists
DROP TYPE IF EXISTS address;

-- Cascade drop
DROP TYPE address CASCADE;
```

#### CREATE INDEX Examples

```sql
-- B-tree index (default)
CREATE INDEX idx_employees_name ON employees(name);

-- Multi-column index
CREATE INDEX idx_employees_dept_name ON employees(department_id, name);

-- Unique index
CREATE UNIQUE INDEX idx_employees_email ON employees(email);

-- GIN index (for JSONB, arrays, full-text search)
CREATE INDEX idx_data_gin ON session_data USING GIN (data);

-- GiST index (for geometric types, range types)
CREATE INDEX idx_location_gist ON locations USING GIST (position);

-- BRIN index (for range queries on large tables)
CREATE INDEX idx_orders_date_brin ON orders USING BRIN (order_date);

-- HASH index
CREATE INDEX idx_users_hash ON users USING HASH (username);

-- SPGiST index (for spatial data)
CREATE INDEX idx_points_spgist ON locations USING SPGIST (point);
```

#### DROP INDEX Examples

```sql
-- Drop index
DROP INDEX idx_employees_name;

-- Drop if exists
DROP INDEX IF EXISTS idx_employees_name;
```

#### REINDEX Examples

```sql
-- Reindex table
REINDEX TABLE employees;

-- Reindex specific index
REINDEX INDEX idx_employees_name;

-- Reindex entire database
REINDEX DATABASE mydb;
```

#### CREATE SCHEMA Examples

```sql
-- Create schema
CREATE SCHEMA hr;
```

#### DROP SCHEMA Examples

```sql
-- Drop schema
DROP SCHEMA hr;

-- Drop if exists
DROP SCHEMA IF EXISTS hr;

-- Cascade drop (drop all objects in schema)
DROP SCHEMA hr CASCADE;
```

---

## Quick Start

### 1. Build the Project

```bash
# Clone repository
git clone <repository-url>
cd pgcastor

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

### Install

After building, you can install the binaries, libraries, and configuration files to a specified directory.

#### Install to Default Location (build/install)

```bash
mkdir -p build && cd build
cmake .. -DPOSTGRES_INSTALL_DIR=/path/to/pgsql
make -j4
make install
```

This installs to `build/install/` directory.

#### Install to Custom Location

```bash
mkdir -p build && cd build
cmake .. -DPOSTGRES_INSTALL_DIR=/path/to/pgsql -DINSTALL_PREFIX=/opt/pgcastor
make -j4
make install
```

#### Install Directory Structure

```
${INSTALL_PREFIX}/
├── bin/
│   ├── capture
│   ├── integrate
│   ├── receivepglog
│   ├── xmanager
│   └── xscsci
├── lib/
│   └── libpgcastor.so
├── config/
│   └── sample/
│       ├── capture.cfg.sample
│       ├── integrate.cfg.sample
│       ├── manager.cfg.sample
│       └── receivelog.cfg.sample
└── env                    # Environment setup script
```

#### Environment Setup

After installation, source the `env` file to set up environment variables:

```bash
# Source the environment file
source /path/to/install/env

# Or add to ~/.bashrc for persistence
echo "source /path/to/install/env" >> ~/.bashrc
```

The `env` file sets:
- `PGCASTOR` - Installation directory path
- `LD_LIBRARY_PATH` - Includes PostgreSQL lib and PGCastor lib directories

---

## Deployment Guide

### Environment Setup

```bash
# Add to ~/.bashrc or ~/.zshrc
export PGCASTOR=/path/to/pgcastor/install
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

| File | Purpose | Description |
|------|---------|-------------|
| `etc/sample/capture.cfg.sample` | Capture configuration | Capture WAL changes from source database |
| `etc/sample/integrate.cfg.sample` | Integrate configuration | Apply changes to target database |
| `etc/sample/xmanager.cfg.sample` | Manager configuration | Manage and coordinate all jobs |
| `etc/sample/receivelog.cfg.sample` | WAL receiver configuration | Receive and archive WAL logs |

---

### capture.cfg.sample Parameters

Capture configuration file for capturing changes from source database.

#### Basic Configuration

| Parameter | Description | Example | Required |
|-----------|-------------|---------|----------|
| `jobname` | Job name | `capture` | ✓ |
| `log_dir` | Log directory (must exist, defaults to `data/log` if not configured) | `/opt/pgcastor/log` | |
| `log_level` | Log level (`debug`/`info`/`warning`/`error`) | `info` | |
| `data` | Working directory | `/opt/pgcastor/capturedata` | ✓ |

#### Database Connection

| Parameter | Description | Example | Required |
|-----------|-------------|---------|----------|
| `url` | Database connection string | `"port=5432 dbname=postgres user=postgres"` | ✓ |
| `wal_dir` | PostgreSQL WAL directory | `/opt/postgresql/data/pg_wal` | ✓ |
| `dbtype` | Database type (currently supports `postgres`) | `postgres` | ✓ |
| `dbversion` | PostgreSQL version (`12`/`13`/`14`) | `12` | ✓ |

#### Sync Configuration

| Parameter | Description | Example | Required |
|-----------|-------------|---------|----------|
| `catalog_schema` | Schema for sync status tables | `pgcastor` | ✓ |
| `ddl` | Enable DDL sync (`1`=on, `0`=off) | `1` | |
| `table` | Tables to sync (format: `schema.table`) | `"public.test"` | ✓ |
| `tableexclude` | Tables to exclude (can be multiple lines) | `*.*` | |
| `addtablepattern` | Table pattern for auto-capture via CREATE TABLE | `"public.%"` | |

#### Performance Tuning

| Parameter | Description | Example | Required |
|-----------|-------------|---------|----------|
| `trail_max_size` | Trail file size (MB) | `16` | ✓ |
| `capture_buffer` | Max memory for capture job (MB) | `8192` | ✓ |
| `max_work_per_refresh` | Threads for existing data sync | `10` | ✓ |
| `max_page_per_refreshsharding` | Existing table sharding threshold | `10000` | ✓ |

#### Initial Sync

| Parameter | Description | Example | Required |
|-----------|-------------|---------|----------|
| `refreshstragety` | Enable existing data sync (`0`=off/`1`=on) | `0` | |

#### Other Options

| Parameter | Description | Example | Required |
|-----------|-------------|---------|----------|
| `compress_algorithm` | Compression algorithm (for existing data sync, supports `bzip2`) | `bzip2` | |
| `compatibility` | Compatibility version (Trail file format) | `10` | |

---

### integrate.cfg.sample Parameters

Integrate configuration file for applying captured changes to target database.

#### Basic Configuration

| Parameter | Description | Example | Required |
|-----------|-------------|---------|----------|
| `jobname` | Job name | `integrate` | ✓ |
| `log_dir` | Log directory (must exist, defaults to `data/log` if not configured) | `/opt/pgcastor/log` | |
| `log_level` | Log level (`info`/`debug`) | `info` | |
| `data` | Working directory | `/opt/pgcastor/integratedata` | ✓ |

#### Database Connection

| Parameter | Description | Example | Required |
|-----------|-------------|---------|----------|
| `url` | Target database connection string | `"port=5438 dbname=postgres user=postgres"` | ✓ |

#### Trail Files

| Parameter | Description | Example | Required |
|-----------|-------------|---------|----------|
| `trail_dir` | Trail files directory (should be capture's data directory) | `/opt/pgcastor/capturedata` | ✓ |
| `trail_max_size` | Trail file size (MB, **must match capture**) | `16` | ✓ |

#### Sync Configuration

| Parameter | Description | Example | Required |
|-----------|-------------|---------|----------|
| `catalog_schema` | Status table schema during init (recommend creating dedicated schema) | `pgcastor` | ✓ |

#### Performance Tuning

| Parameter | Description | Example | Required |
|-----------|-------------|---------|----------|
| `integrate_buffer` | Large transaction sharding memory (MB) | `128` | ✓ |
| `max_work_per_refresh` | Threads for existing data apply | `10` | ✓ |
| `integrate_method` | Integration apply mode (`burst`/`empty`) | `burst` | |

#### Transaction Processing

| Parameter | Description | Example | Required |
|-----------|-------------|---------|----------|
| `mergetxn` | Enable transaction merge (`0`=no/`1`=yes, defaults to `0`) | `1` | |
| `txbundlesize` | Transaction merge threshold - statements per transaction (range: 0~10000000) | `1000` | |

#### Data Processing

| Parameter | Description | Example | Required |
|-----------|-------------|---------|----------|
| `truncate_table` | Truncate table when applying existing data (`0`=no/`1`=yes) | `0` | |
| `compress_algorithm` | Compression algorithm (**must match capture config**, `bunzip2`) | `bunzip2` | |
| `compress_level` | Compression level | `9` | |

#### Other Options

| Parameter | Description | Example | Required |
|-----------|-------------|---------|----------|
| `compatibility` | Compatibility version | `10` | |

---

### xmanager.cfg.sample Parameters

Manager configuration file for managing and coordinating all PGCastor jobs.

#### Basic Configuration

| Parameter | Description | Example | Required |
|-----------|-------------|---------|----------|
| `jobname` | Job name | `xmanager` | ✓ |
| `log_dir` | Log directory (must exist, defaults to `data/log` if not configured) | `/opt/pgcastor/log` | |
| `log_level` | Log level (`info`/`debug`) | `info` | |
| `data` | Working directory | `/opt/pgcastor/xmanagerdata` | ✓ |

#### Network Configuration

| Parameter | Description | Example | Required |
|-----------|-------------|---------|----------|
| `host` | xmanager local IP address | `127.0.0.1` | ✓ |
| `port` | Network port | `6543` | ✓ |

#### TCP Keepalive

| Parameter | Description | Example | Required |
|-----------|-------------|---------|----------|
| `tcp_keepalive` | Enable TCP keepalive (`0`=off/`1`=on) | `1` | |
| `tcp_user_timeout` | Timeout for waiting ACK from peer (ms, range: 0~300000) | `60000` | |
| `tcp_keepalives_idle` | Seconds before sending keepalive probe after idle (range: 0~300) | `30` | |
| `tcp_keepalives_interval` | Keepalive probe interval in seconds (range: 0~300) | `3` | |
| `tcp_keepalives_count` | Number of keepalive probes (range: 0~100) | `10` | |

#### Other Options

| Parameter | Description | Example | Required |
|-----------|-------------|---------|----------|
| `unixdomaindir` | Unix domain socket directory | `/tmp` | |

---

### receivelog.cfg.sample Parameters

WAL receiver configuration file for receiving and archiving PostgreSQL WAL logs.

#### Log Configuration

| Parameter | Description | Example | Required |
|-----------|-------------|---------|----------|
| `log_level` | Log level | `info` | |
| `log_dir` | Log directory | `/opt/receivewaldata/log` | |

#### Working Directory

| Parameter | Description | Example | Required |
|-----------|-------------|---------|----------|
| `data` | Working directory/transaction log directory | `/opt/receivewaldata` | ✓ |

#### Sync Parameters

| Parameter | Description | Example | Required |
|-----------|-------------|---------|----------|
| `timeline` | Sync starting timeline | `1` | |
| `startpos` | Sync starting LSN | `"0/02000000"` | |

#### Connection Configuration

| Parameter | Description | Example | Required |
|-----------|-------------|---------|----------|
| `primary_conn_info` | Source database connection string | `"port=9527 dbname=postgres user=postgres"` | ✓ |

#### Other Options

| Parameter | Description | Example | Required |
|-----------|-------------|---------|----------|
| `restore_command` | Command to copy missing logs from source (`%f` `%p` are fixed format) | `"scp user@ip:/opt/pg/archive/%f %p"` | |
| `slot_name` | Replication slot name (must exist in database, can be empty) | `"recvwal"` | |

---

### Configuration Notes

#### 1. capture and integrate Parameter Correspondence

The following parameters must be consistent between capture and integrate:

| Parameter | Notes |
|-----------|-------|
| `trail_max_size` | Trail file size must be exactly the same |
| `compress_algorithm` | Compression algorithms must correspond (capture uses `bzip2`, integrate uses `bunzip2`) |

#### 2. Performance Tuning Recommendations

- **capture_buffer**: Recommend setting to 50%-70% of available system memory
- **max_work_per_refresh**: Set based on CPU cores, recommend 1-2x core count
- **txbundlesize**: Larger transaction merge threshold improves performance but increases recovery time

#### 3. Common Configuration Errors

- ❌ `trail_max_size` mismatch between capture and integrate
- ❌ Forgot to create `log_dir` directory causing startup failure
- ❌ Incorrect `wal_dir` path preventing WAL reading
- ❌ Wrong `table` parameter format (should be `schema.table`)
- ❌ `catalog_schema` schema does not exist

---

## Directory Structure

```
pgcastor/
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
