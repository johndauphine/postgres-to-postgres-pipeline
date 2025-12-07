# Migration Performance Report

## Key Finding: Parallelism Tuning is Platform-Dependent

**High parallelism (64 tasks) works well on native environments (Mac ARM64) but causes severe performance degradation on virtualized environments (WSL2/Docker Desktop) due to I/O contention.**

| Platform | Optimal Tasks | Throughput | Notes |
|----------|---------------|------------|-------|
| Mac M3 Max (native ARM) | 64 | 531K rows/sec | Native virtualization, minimal overhead |
| WSL2/Docker Desktop | 8 | ~204K rows/sec | I/O virtualization creates contention |
| WSL2 with 64 tasks | 64 | ~74K rows/sec | **3x SLOWER** due to I/O bottleneck |

---

## Latest Run: StackOverflow2013 - 2025-12-07T01:55:07Z (Mac M3 Max Optimized)

**Run ID:** `manual__2025-12-07T01:55:06.911700+00:00`
**Final Status:** Success (validation DAG triggered and passed)
**Total Duration:** 200.6 seconds (~3.3 minutes)
**Dataset:** StackOverflow2013 (~106.5M rows)

### Executive Summary

| Metric | Value |
|--------|-------|
| **Total Rows Migrated** | 106,534,545 |
| **Total Duration** | 200.6 seconds |
| **Throughput** | ~531,000 rows/sec |
| **Partition Tasks** | 36 parallel partitions |
| **Validation** | 6/6 tables passed |

### Execution Environment

#### Host System
| Property | Value |
|----------|-------|
| OS | Darwin 25.1.0 (macOS Sequoia) |
| Architecture | arm64 (native) |
| CPU | Apple M3 Max |
| Host Memory | 36 GB |

#### Docker Resources
| Property | Value |
|----------|-------|
| Docker CPUs | 14 |
| Docker Memory | 16 GB |

#### Container Configuration
| Container | CPUs | Memory |
|-----------|------|--------|
| PostgreSQL Source (native ARM) | 6 | 4 GB |
| PostgreSQL Target (native ARM) | 6 | 4 GB |

#### Database Versions
| Database | Version |
|----------|---------|
| PostgreSQL Source | PostgreSQL 16-alpine on aarch64 |
| PostgreSQL Target | PostgreSQL 16-alpine on aarch64 |

### Source Database (StackOverflow2013)

| Table | Rows |
|-------|------|
| Votes | 52,928,720 |
| Comments | 24,534,730 |
| Posts | 17,142,169 |
| Badges | 8,042,005 |
| Users | 2,465,713 |
| PostLinks | 1,421,208 |
| **Total** | **106,534,545** |

### Airflow Parallelism Configuration

| Setting | Value |
|---------|-------|
| `AIRFLOW__CORE__PARALLELISM` | 128 |
| `AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG` | 64 |
| `AIRFLOW__CELERY__WORKER_CONCURRENCY` | 64 |
| `AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC` | 5 |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE` | 10 |

### PostgreSQL Tuning (Target)

| Setting | Value |
|---------|-------|
| `max_connections` | 200 |
| `shared_buffers` | 1GB |
| `effective_cache_size` | 2GB |
| `maintenance_work_mem` | 256MB |
| `wal_buffers` | 64MB |
| `max_wal_size` | 4GB |
| `synchronous_commit` | off |

### Data Verification

| Table | Source Rows | Target Rows | Match |
|-------|-------------|-------------|-------|
| votes | 52,928,720 | 52,928,720 | Yes |
| comments | 24,534,730 | 24,534,730 | Yes |
| posts | 17,142,169 | 17,142,169 | Yes |
| badges | 8,042,005 | 8,042,005 | Yes |
| users | 2,465,713 | 2,465,713 | Yes |
| postlinks | 1,421,208 | 1,421,208 | Yes |

---

## Performance Comparison

### Before vs After Optimization

| Metric | Before (8 max tasks) | After (64 max tasks) | Improvement |
|--------|---------------------|---------------------|-------------|
| Duration | 299.7s | 200.6s | **33% faster** |
| Throughput | 355K rows/sec | 531K rows/sec | **50% higher** |
| Max Concurrent Tasks | 8 | 64 | 8x |

### PostgreSQL vs MSSQL Source Comparison

| Pipeline | Source | Rows | Time | Throughput | Notes |
|----------|--------|------|------|------------|-------|
| PG→PG (native ARM) | PostgreSQL | 106M | 200.6s | 531K/sec | Native ARM, no emulation |
| MSSQL→PG (Rosetta) | SQL Server | 106M | 318s | 321K/sec | x86 emulation overhead |

**Key Finding:** Native PostgreSQL-to-PostgreSQL is 66% faster than MSSQL-to-PostgreSQL due to:
1. No Rosetta 2 x86 emulation overhead
2. Both databases run native ARM64
3. Same wire protocol (no translation needed)

---

## Historical Run: StackOverflow2013 - 2025-12-07T01:45:26Z (Baseline)

**Run ID:** `manual__2025-12-07T01:45:26.011321+00:00`
**Final Status:** Success
**Total Duration:** 299.7 seconds (~5 minutes)
**Throughput:** ~355,000 rows/sec

This was the baseline run with `max_active_tasks=8`, causing partition tasks to queue.

---

## Historical Run: StackOverflow2010 - 2025-11-23T17:18:15Z

**Run ID:** `manual__2025-11-23T17:18:15.048851+00:00`
**Final Status:** Success (validation DAG triggered and passed)
**Total Duration:** 00:02:33.58 (153.58s)

### Table Transfer Performance

| Table | Duration | Rows | Rows/sec |
|-------|----------|------|----------|
| Users | 8.04s | 299,398 | 37,242 |
| Posts | 47.96s | 3,729,195 | 77,755 |
| Votes | 1:55.43 | 10,143,364 | 87,874 |
| Badges | 10.20s | 1,102,019 | 108,086 |
| Comments | 59.68s | 3,875,183 | 64,938 |
| PostLinks | 1.60s | 161,519 | 100,873 |

**Total Rows Migrated:** ~19.3M (~125,700 rows/sec overall)

---

## Performance Optimization Summary

### Optimizations Applied

1. **Increased Airflow Parallelism** - 64 concurrent tasks per DAG (was 8)
2. **PostgreSQL Tuning** - Optimized for bulk loading
   - Source: 200 max connections, 1GB shared_buffers
   - Target: synchronous_commit=off, 4GB max_wal_size
3. **Connection Pooling** - SQL Alchemy pool size increased
4. **Scheduler Tuning** - 5s heartbeat for faster task pickup

### Recommendations for Production

1. **Native ARM databases** - Avoid x86 emulation when possible
2. **Match CPU allocation** - Both source and target get 6 CPUs each
3. **High parallelism** - 64+ concurrent tasks for large datasets
4. **Disable sync commit** - For bulk loading (enable after migration)
5. **Large WAL size** - 4GB reduces checkpoint frequency

---

## Hardware Comparison

### Tested Systems

| System | CPU | Cores | RAM | OS | Docker Backend |
|--------|-----|-------|-----|-----|----------------|
| Mac | Apple M3 Max | 14 | 36 GB | macOS Sequoia | Docker Desktop (native ARM) |
| Windows | Intel/AMD | 16 | TBD | Windows 11 | Docker Desktop (WSL2) |

### Expected Performance by Platform

| Pipeline | Mac (M3 Max) | Windows (16-core) | Notes |
|----------|--------------|-------------------|-------|
| **PG→PG** | 531K rows/sec | ~600K+ rows/sec | Native x86 on Windows |
| **MSSQL→PG** | 321K rows/sec | ~500K+ rows/sec | No Rosetta overhead on Windows |

### Windows Setup

#### WSL2 Configuration (Recommended)

Create or edit `%UserProfile%\.wslconfig`:

```ini
[wsl2]
memory=16GB
processors=12
swap=4GB
```

Restart WSL after changes:
```powershell
wsl --shutdown
```

#### Docker Desktop Settings

1. Enable WSL2 backend (Settings → General → Use WSL 2 based engine)
2. Resources → WSL Integration → Enable for your distro
3. Allocate at least 12 CPUs and 12GB RAM

#### Running on Windows

```powershell
# Pull latest optimizations
git pull origin main

# Start databases
docker-compose down
docker-compose up -d

# Start Airflow
astro dev restart

# Connect databases to Airflow network
# On Windows, run this in PowerShell:
$network = docker network ls --format "{{.Name}}" | Select-String "airflow"
docker network connect $network postgres-source
docker network connect $network postgres-target

# Trigger migration
$scheduler = docker ps --format "{{.Names}}" | Select-String "scheduler"
docker exec $scheduler airflow dags trigger postgres_to_postgres_migration
```

### Why Windows May Be Faster for SQL Server

1. **Native x86 SQL Server** - No Rosetta 2 emulation (Mac ARM must emulate x86)
2. **More CPU cores** - 16 cores vs 14 cores
3. **WSL2 performance** - Near-native Linux performance for containers
4. **Native Docker volumes** - Faster disk I/O than Mac's virtualized filesystem

---

## WSL2 Parallelism Investigation (2025-12-07)

### Background

After achieving 531K rows/sec on Mac M3 Max with 64 concurrent tasks, we attempted to replicate these settings on WSL2/Docker Desktop. The result was unexpected.

### Test Results

| Configuration | Concurrent Tasks | Time | Throughput | Result |
|---------------|------------------|------|------------|--------|
| WSL2 Baseline | 8 | ~8.8 min | ~204K rows/sec | Optimal for WSL2 |
| WSL2 High Parallelism | 64 | 24+ min | ~74K rows/sec | **3x SLOWER** |
| Mac M3 Max | 64 | 3.3 min | 531K rows/sec | Native ARM optimal |

### Root Cause Analysis

**WSL2/Docker Desktop has significant I/O virtualization overhead:**

1. **Filesystem Layer**: WSL2 uses 9P protocol for filesystem access between Windows and Linux, adding latency
2. **Docker Volume Mounts**: Each read/write crosses multiple abstraction layers
3. **I/O Contention**: High parallelism (64 tasks) causes all tasks to compete for the same I/O bandwidth
4. **CPU Context Switching**: More concurrent tasks = more context switches = more overhead

**Mac's Virtualization Framework is more efficient:**
- Apple's native ARM64 hypervisor has minimal overhead
- Docker Desktop on Mac uses Apple's Virtualization.framework
- No translation layer (unlike WSL2's 9P protocol)

### Optimal Settings by Platform

#### Mac M3 Max / Apple Silicon (32GB+ RAM)
```bash
AIRFLOW__CORE__PARALLELISM=128
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=64
MAX_PARTITIONS=12
PG_POOL_MAXCONN=16
```

#### WSL2 / Docker Desktop (32GB RAM)
```bash
AIRFLOW__CORE__PARALLELISM=16
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=8
MAX_PARTITIONS=8
PG_POOL_MAXCONN=8
```

### Key Takeaway

**More parallelism is not always better.** Platform-specific tuning is essential:
- Native/bare-metal: Higher parallelism scales well
- Virtualized (WSL2, VirtualBox, etc.): Lower parallelism reduces I/O contention
