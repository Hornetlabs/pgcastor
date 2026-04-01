# pgcastor-pro Run and Test Guide

## Overview

This skill provides comprehensive instructions for running and testing the pgcastor-pro (XSynch) PostgreSQL replication toolkit after successful compilation.

## Prerequisites

### Completed Steps
- Project compiled successfully (see `compile.md`)
- Binaries located in `./install/` directory
- PostgreSQL instances running on:
  - Source DB: port 5432
  - Target DB: port 5433

### Required Files Structure
```
install/
├── capture
├── integrate
├── xmanager
├── xscsci
└── config/
    ├── sample/
    │   ├── capture.cfg.sample
    │   ├── integrate.cfg.sample
    │   └── manager.cfg.sample
    ├── manager_mgr1.cfg
    ├── capture_cap1.cfg
    └── integrate_int1.cfg
```

## Environment Setup

### 1. Set Environment Variables

```bash
export LD_LIBRARY_PATH=/home/liu/pg/pgsql/lib:/home/liu/pgcastor/pgcastor-pro/lib:/home/liu/pgcastor/pgcastor-pro/interfaces/lib
export XSYNCH=/home/liu/pgcastor/pgcastor-pro/install
```

Or source the environment file:
```bash
source /home/liu/pgcastor/pgcastor-pro/env
```

### 2. Navigate to Install Directory

```bash
cd /home/liu/pgcastor/pgcastor-pro/install
```

## Pre-Test Cleanup

### Check for Running Processes

```bash
ps aux | grep -E "(xmanager|capture|integrate)" | grep -v grep
```

If processes are found, stop them:
```bash
killall -9 xmanager
killall -9 capture
killall -9 integrate
```

### Clean Old Data Directories

**⚠️ IMPORTANT: Ask user for confirmation before deleting!**

```bash
# List directories that will be deleted
ls -la /home/liu/pgcastor/pgcastor-pro/integratedata 2>/dev/null
ls -la /home/liu/pgcastor/pgcastor-pro/capturedata 2>/dev/null
ls -la /home/liu/pgcastor/pgcastor-pro/xdata 2>/dev/null

# After user confirmation, delete:
rm -rf /home/liu/pgcastor/pgcastor-pro/integratedata
rm -rf /home/liu/pgcastor/pgcastor-pro/capturedata
rm -rf /home/liu/pgcastor/pgcastor-pro/xdata
```

## Testing Components

### Test 1: xmanager (Manager Process)

#### Create xmanager Configuration
```bash
./xscsci
```
In the interactive prompt:
```
xscsci=> create manager mgr1
xscsci=> exit
```

Or manually copy from sample:
```bash
cp config/sample/manager.cfg.sample config/manager_mgr1.cfg
```

#### Initialize xmanager
```bash
./xmanager -f config/manager_mgr1.cfg init
```

**Expected Output:**
```
---------xmanager config begin--------------
jobname:    xmgr
data:       /home/liu/pgcastor/pgcastor-pro/xdata
logdir:     NULL
loglevel:   info
host:               127.0.0.1
unixdomaindir:      /tmp
port:       6543
...
---------xmanager config   end--------------
-------------------------------------
|                                   |
|           xsynch init success     |
|                                   |
-------------------------------------
```

#### Start xmanager
```bash
./xmanager -f config/manager_mgr1.cfg start
```

#### Verify xmanager Running
```bash
# Check process
ps aux | grep xmanager | grep -v grep

# Check port listening
ss -tlnp | grep 6543

# Check data directory
ls -la /home/liu/pgcastor/pgcastor-pro/xdata/
```

**Success Criteria:**
- Process running (e.g., `./xmanager -f config/manager_mgr1.cfg start`)
- Port 6543 listening on 127.0.0.1
- xdata directory created with log, metric, proc.lock files

### Test 2: capture (Capture Process)

#### Create capture Configuration
```bash
./xscsci
```
In the interactive prompt:
```
xscsci=> create capture cap1
xscsci=> exit
```

Or manually copy from sample:
```bash
cp config/sample/capture.cfg.sample config/capture_cap1.cfg
```

#### Initialize capture
```bash
./capture -f config/capture_cap1.cfg init
```

**Expected Output:**
```
---------capture config begin--------------
ddl:            off
szloglevel:     info
data:           /home/liu/pgcastor/pgcastor-pro/capturedata
url:            port=5432 dbname=postgres user=liu
waldir:         /home/liu/pg/pgsql/data/pg_wal
dbtype:         postgres
dbversion:      12
xsynchschema:   ripple
logdir:   NULL
tbinclude:*.*
addtablepattern:          *.*
---------capture config   end--------------
-------------------------------------
|                                   |
|           xsynch init success     |
|                                   |
-------------------------------------
```

#### Start capture
```bash
./capture -f config/capture_cap1.cfg start
```

#### Verify capture Running
```bash
# Check process
ps aux | grep capture | grep -v grep

# Check data directory
ls -la /home/liu/pgcastor/pgcastor-pro/capturedata/
```

**Success Criteria:**
- Process running (e.g., `./capture -f config/capture_cap1.cfg start`)
- capturedata directory created with:
  - catalog/
  - chk/
  - filter/
  - log/
  - onlinerefresh/
  - refresh/
  - stat/
  - trail/
  - capture.stat
  - proc.lock
  - ripple.ctrl

### Test 3: integrate (Integrate Process)

#### Create integrate Configuration
```bash
./xscsci
```
In the interactive prompt:
```
xscsci=> create integrate int1
xscsci=> exit
```

Or manually copy from sample:
```bash
cp config/sample/integrate.cfg.sample config/integrate_int1.cfg
```

#### Initialize integrate
```bash
./integrate -f config/integrate_int1.cfg init
```

**Expected Output:**
```
---------integrate config begin--------------
szloglevel:     info
data:           /home/liu/pgcastor/pgcastor-pro/integratedata
traildir:       /home/liu/pgcastor/pgcastor-pro/capturedata
url:            port=5433 dbname=postgres user=liu
---------integrate config   end--------------
-------------------------------------
|                                   |
|           xsynch init success     |
|                                   |
-------------------------------------
```

#### Start integrate
```bash
./integrate -f config/integrate_int1.cfg start
```

**Note:** You may see a NOTICE about existing tables, which is normal.

#### Verify integrate Running
```bash
# Check process
ps aux | grep integrate | grep -v grep

# Check data directory
ls -la /home/liu/pgcastor/pgcastor-pro/integratedata/
```

**Success Criteria:**
- Process running (e.g., `./integrate -f config/integrate_int1.cfg start`)
- integratedata directory created with:
  - chk/
  - filter/
  - log/
  - onlinerefresh/
  - refresh/
  - stat/
  - trail/
  - proc.lock

### Test 4: Data Synchronization Test

This is the critical end-to-end test to verify data replication works correctly.

#### Step 1: Create Test Table in Source DB (port 5432)

```bash
/home/liu/pg/pgsql/bin/psql -d postgres -c "create table test_on_test(id int);"
```

**Expected Output:**
```
CREATE TABLE
```

#### Step 2: Insert Test Data in Source DB

```bash
/home/liu/pg/pgsql/bin/psql -d postgres -c "insert into test_on_test select 1;"
```

**Expected Output:**
```
INSERT 0 1
```

#### Step 3: Verify Data in Target DB (port 5433) with Retry

```bash
for i in 1 2 3; do
  echo "=== Attempt $i ==="
  result=$(/home/liu/pg/pgsql/bin/psql -d postgres -p 5433 -t -c "select * from test_on_test;" 2>&1)
  echo "$result"
  
  # Check if we got data (not empty and not an error)
  if echo "$result" | grep -q "1"; then
    echo "SUCCESS: Data synchronized!"
    exit 0
  fi
  
  if [ $i -lt 3 ]; then
    echo "Data not found, waiting 3 seconds..."
    sleep 3
  fi
done

echo "FAILED: Data not synchronized after 3 attempts"
exit 1
```

**Expected Output:**
```
=== Attempt 1 ===
  1

SUCCESS: Data synchronized!
```

**Success Criteria:**
- Table created in target DB (port 5433)
- Data replicated from source to target
- Query returns the inserted value (1)

## Verification Summary

### Final State Check

Run these commands to verify all components are running correctly:

```bash
# Check all processes
ps aux | grep -E "(xmanager|capture|integrate)" | grep -v grep

# Check xmanager port
ss -tlnp | grep 6543

# Check data directories
ls -la /home/liu/pgcastor/pgcastor-pro/xdata/
ls -la /home/liu/pgcastor/pgcastor-pro/capturedata/
ls -la /home/liu/pgcastor/pgcastor-pro/integratedata/

# Check configuration files
ls -la /home/liu/pgcastor/pgcastor-pro/install/config/*.cfg
```

### Expected Final State

| Component | Status | Process | Port | Data Directory |
|-----------|--------|---------|------|----------------|
| xmanager | Running | ✓ | 6543 | xdata/ |
| capture | Running | ✓ | - | capturedata/ |
| integrate | Running | ✓ | - | integratedata/ |
| Data Sync | Working | - | - | - |

## Interactive CLI (xscsci)

The `xscsci` tool provides an interactive CLI for managing XSynch jobs.

### Starting xscsci

```bash
./xscsci
```

### Available Commands

```
create manager <name>      Create manager job
create capture <name>      Create capture job
create integrate <name>    Create integrate job

init manager <name>        Initialize manager
init capture <name>        Initialize capture
init integrate <name>      Initialize integrate

start manager <name>       Start manager
start capture <name>       Start capture
start integrate <name>     Start integrate

stop manager <name>        Stop manager
stop capture <name>        Stop capture
stop integrate <name>      Stop integrate

status manager <name>      Show manager status
status capture <name>      Show capture status
status integrate <name>    Show integrate status

info manager <name>        Show manager info
info capture <name>        Show capture info
info integrate <name>      Show integrate info

drop manager <name>        Drop manager job
drop capture <name>        Drop capture job
drop integrate <name>      Drop integrate job

help                       Show help
exit                       Exit xscsci
```

## Troubleshooting

### Process Won't Start

1. **Check logs:**
   ```bash
   cat /home/liu/pgcastor/pgcastor-pro/xdata/log/*.log
   cat /home/liu/pgcastor/pgcastor-pro/capturedata/log/*.log
   cat /home/liu/pgcastor/pgcastor-pro/integratedata/log/*.log
   ```

2. **Check PostgreSQL connectivity:**
   ```bash
   /home/liu/pg/pgsql/bin/psql -p 5432 -c "SELECT version();"
   /home/liu/pg/pgsql/bin/psql -p 5433 -c "SELECT version();"
   ```

3. **Check permissions:**
   ```bash
   ls -la /home/liu/pg/pgsql/data/pg_wal/
   ```

### Data Not Synchronizing

1. **Verify all processes running:**
   ```bash
   ps aux | grep -E "(xmanager|capture|integrate)"
   ```

2. **Check capture is reading WAL:**
   ```bash
   ls -la /home/liu/pgcastor/pgcastor-pro/capturedata/trail/
   ```

3. **Check integrate is reading trails:**
   ```bash
   ls -la /home/liu/pgcastor/pgcastor-pro/integratedata/trail/
   ```

4. **Check PostgreSQL replication slots:**
   ```bash
   /home/liu/pg/pgsql/bin/psql -p 5432 -c "SELECT * FROM pg_replication_slots;"
   ```

### Port Already in Use

If port 6543 is already in use:

```bash
# Find process using the port
ss -tlnp | grep 6543

# Kill the process
kill -9 <PID>
```

### Clean Restart

To perform a complete clean restart:

```bash
# Stop all processes
killall -9 xmanager capture integrate

# Remove data directories (ASK USER FIRST!)
rm -rf /home/liu/pgcastor/pgcastor-pro/xdata
rm -rf /home/liu/pgcastor/pgcastor-pro/capturedata
rm -rf /home/liu/pgcastor/pgcastor-pro/integratedata

# Truncate sync status table
/home/liu/pg/pgsql/bin/psql -d postgres -p 5433 -c 'truncate xsynch.sync_status;'

# Restart from initialization
```

## Configuration Files

### capture.cfg

Key parameters:
- `jobname`: Job identifier
- `url`: Source database connection string
- `data`: Working directory for capture data
- `wal_dir`: PostgreSQL WAL directory
- `dbtype`: Database type (postgres)
- `dbversion`: PostgreSQL version (12, 13, or 14)
- `catalog_schema`: Schema for sync status tables
- `table`: Tables to capture (e.g., `*.*` for all)
- `ddl`: Enable DDL synchronization (1=on, 0=off)

### integrate.cfg

Key parameters:
- `jobname`: Job identifier
- `url`: Target database connection string
- `data`: Working directory for integrate data
- `trail_dir`: Directory containing trail files from capture
- `catalog_schema`: Schema for sync status tables
- `max_work_per_refresh`: Worker threads for initial sync

### manager.cfg

Key parameters:
- `jobname`: Manager identifier
- `data`: Working directory for manager
- `port`: Network port (default: 6543)
- `host`: Bind address (default: 127.0.0.1)
- `tcp_keepalive`: Enable TCP keepalive
- `unixdomaindir`: Unix domain socket directory

## Testing Checklist

Before considering the system fully tested:

- [ ] xmanager initialized and started
- [ ] xmanager process running
- [ ] xmanager port 6543 listening
- [ ] capture initialized and started
- [ ] capture process running
- [ ] capture data directory created
- [ ] integrate initialized and started
- [ ] integrate process running
- [ ] integrate data directory created
- [ ] Test table created in source DB
- [ ] Test data inserted in source DB
- [ ] Test data verified in target DB
- [ ] All three processes still running after test

## Additional Resources

- Compilation Guide: `compile.md`
- Project Documentation: `/home/liu/pgcastor/pgcastor-pro/README.md`
- Architecture Guide: `/home/liu/pgcastor/pgcastor-pro/AGENTS.md`
- Clean Script: `/home/liu/pgcastor/pgcastor-pro/clean.sh`
- Environment File: `/home/liu/pgcastor/pgcastor-pro/env`

---

*This skill was generated from successful testing on April 1, 2026*
