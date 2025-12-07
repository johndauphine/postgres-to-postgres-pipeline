# Migration Performance Report

## Latest Run: StackOverflow2013 - 2025-12-07T01:55:07Z (Optimized)

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
