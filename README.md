<div align="center">
  <h1 align="center">AURA-EPICS</h1>
  <p align="center">
    <strong>Autonomous Universal Relational Archiver for EPICS Control Systems</strong>
  </p>
  <p align="center">
    <em>High-throughput, memory-safe data archiving for particle accelerator instrumentation.</em>
  </p>
  <p align="center">
    <a href="#motivation">Motivation</a> •
    <a href="#why-aura">Why AURA</a> •
    <a href="#features">Features</a> •
    <a href="#architecture">Architecture</a> •
    <a href="#performance">Performance</a> •
    <a href="#quickstart">Quickstart</a> •
    <a href="#configuration">Configuration</a> •
    <a href="#database-schema">Database Schema</a> •
    <a href="#normative-types">Normative Types</a> •
    <a href="#license">License</a>
  </p>
  <p align="center">
    <img src="https://img.shields.io/badge/language-Rust_2024-orange?style=flat-square&logo=rust" alt="Rust">
    <img src="https://img.shields.io/badge/runtime-Tokio-blue?style=flat-square" alt="Tokio">
    <img src="https://img.shields.io/badge/storage-TimescaleDB-green?style=flat-square" alt="TimescaleDB">
    <img src="https://img.shields.io/badge/protocol-PVAccess_(pvxs)-blueviolet?style=flat-square" alt="PVAccess">
    <img src="https://img.shields.io/badge/license-MIT-lightgrey?style=flat-square" alt="License">
  </p>
</div>

---

## Motivation

Large-scale physics facilities such as particle accelerators rely on EPICS (Experimental Physics and Industrial Control
System) to orchestrate thousands of sensors, magnets, and diagnostic instruments. Each device publishes its state as a
**Process Variable (PV)** : a real-time data stream that must be archived continuously for post-mortem analysis, trend
monitoring, and machine protection.

Existing EPICS archivers (such as the Archiver Appliance and CSS RDB Archive Engine) are built on the Java Virtual
Machine and were historically optimized around Channel Access. As facilities widely adopt EPICS 7 and PVAccess (the
next-generation protocol supporting structured types, images, and high-frequency waveforms), these legacy Java-based
architectures face inherent limitations: GC-induced latency spikes, memory overhead from object allocation under heavy
streaming loads, and complex scaling models required to match modern high-throughput pipelines.

**AURA-EPICS** is a ground-up replacement written in Rust, initially designed for
the [PERLE](https://perle-web.ijclab.in2p3.fr/)
accelerator project at IJCLab (CNRS/IN2P3). It provides a zero-GC, multithreaded, PVAccess-native archiving pipeline
designed for a sustained **~1M events/s** on commodity hardware (every quoted figure is produced and published by the
reproducible `aura-bench` harness : see [Performance](#performance)),
supporting all 13 EPICS 7 Normative Types at the protocol layer plus custom structures, with archival pipelines
optimized for the 11 data-bearing PV types (NTURI being an RPC request type and NTAttribute existing strictly as a
component of NTNDArray rather than as standalone archivable PVs).

## Why AURA

|                           | Archiver Appliance                                            | AURA-EPICS                                                                                                                                   |
|---------------------------|---------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| **Base Protocol**         | Channel Access (CA, V3) + PVA (adapted)                       | PVAccess (pvxs, V7)                                                                                                                          |
| **Normative Types**       | Flattened to CA types / Raw blob blocks                       | 11 archivable EPICS 7 NTs destructured + custom structures (images, tables, histograms... out of 13 total)                                   |
| **Runtime**               | JVM (GC pauses, heap tuning)                                  | Rust (zero-GC, zero-cost abstractions)                                                                                                       |
| **Write path**            | Row-by-row INSERT / PlainPB files                             | Binary COPY (parallel, N connections)                                                                                                        |
| **Throughput Bottleneck** | JVM memory pressure & GC pauses under high-frequency monitors | ~1M events/s design target (to be measured and published via aura-bench : see [Performance](https://www.google.com/search?q=%23performance)) |
| **Compression**           | PlainPB + ETL pipeline                                        | Gorilla + delta-of-delta (~15–40× on scalars with 24 h roll-up)                                                                              |
| **Array storage**         | Custom PB files / Flat relational schemas                     | Destructured element-per-row (10× compression)                                                                                               |
| **IOC discovery**         | Static config files                                           | SQL-driven hot-add (no restart)                                                                                                              |
| **Deployment**            | WAR + Tomcat + MySQL + ETL jobs                               | Single binary + TimescaleDB + Redis                                                                                                          |
| **Query layer**           | Custom retrieval servlet                                      | Automatic tier selection (raw → hourly → daily; library layer : REST API in development)                                                     |
| **Retention**             | Manual ETL consolidation                                      | TimescaleDB policies (automatic, per-table)                                                                                                  |

## Features

- **PVAccess-native** : Direct TCP connection to IOCs via the pvxs protocol (V7). No Channel Access gateway required.
  Session multiplexing, automatic reconnection, and heartbeat monitoring are built in.

- **11 archivable Normative Types + custom structures** : NTScalar, NTEnum, NTScalarArray, NTMatrix, NTHistogram,
  NTContinuum, NTNameValue, NTTable, NTNDArray (images), NTMultiChannel, and NTAggregate, plus custom structures. Each
  type is routed to an optimized storage table. Out of the 13 total EPICS 7 Normative Types, NTURI (an RPC request type)
  and NTAttribute (a component-specific type) are intentionally excluded from archival as they are not standalone PVs.

- **Destructured array storage** : Waveforms are stored element-per-row instead of as PostgreSQL arrays, enabling
  TimescaleDB gorilla compression at ~0.6–1.1 bytes/element (~7–13× vs 8 bytes raw).

- **Binary COPY pipeline** : All writes bypass SQL parsing entirely via the PostgreSQL binary COPY protocol. Parallel
  COPY across N dedicated connections for N× write throughput.

- **Zero-copy image ingestion** : NTNDArray camera frames are moved through the pipeline via `std::mem::take`, avoiding
  multi-megabyte allocations per frame.

- **Low-contention shared buffers** : Per-thread `SharedBuffer` guarded by a `parking_lot::Mutex` (one producer + one
  consumer per buffer, so contention is negligible) feeds the store loop. The scalar fast path
  bypasses enum dispatch entirely. PV-id lookups, in contrast, ARE lock-free (`ArcSwap`).

- **Hot PV management** : PVs can be added or removed at runtime via `INSERT INTO pv_config` without restart. PostgreSQL
  `LISTEN/NOTIFY` propagates changes to the archiver within 100 ms.

- **Automatic IOC discovery** : New IOCs added to `ioc_config` are detected via PostgreSQL NOTIFY. The archiver
  establishes TCP sessions and subscribes to all configured PVs automatically.

- **Adaptive chunk tuning** : TimescaleDB chunk intervals are re-calibrated every 5 minutes based on measured
  throughput, keeping active chunk indexes within `shared_buffers`.

## Architecture

```md
┌─────────────────────────────────────────────────────────────────────────┐
│ AURA-EPICS Binary │
│ ------------------- │
│ │
│ ┌───────────┐ ┌──────────────┐ ┌───────────────────────────────┐ │
│ │ │ │ │ │ aura-store │ │
│ │ aura-net │-->│ aura-ingest │--->│ │ │
│ │ │ │ │ │ SharedBuffer ---> store_loop │ │
│ │ pvxs │ │ N threads │ │ | | │ │
│ │ TCP │ │ ScalarDelta │ │ v v │ │
│ │ sessions │ │ fast path │ │ ScalarWriter JsonWriter │ │
│ │ │ │ │ │ StringWriter ImageWriter │ │
│ └───────────┘ └──────────────┘ │ ArrayWriter │ │
│ │ | │ │
│ ┌──────────────┐ │ v │ │
│ │ aura-discover│ │ CopyPool (N connections)     │ │
│ │ │ │ | │ │
│ │ config_poller│ │ v │ │
│ │ pg_notify │ │ Binary COPY ---> TimescaleDB │ │
│ └──────────────┘ └───────────────────────────────┘ │
│ │
│ ┌──────────┐ │
│ │aura-core │ Shared types, config, error handling, PVA codec │
│ └──────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
^ |
| pvxs TCP | discovery
v v commands
┌──────────┐ ┌──────────┐
│ IOCs │ │ Redis │
└──────────┘ └──────────┘
```

### Crate Responsibilities

| Crate             | Role                                                                          |
|-------------------|-------------------------------------------------------------------------------|
| **aura-core**     | Shared types: AuraConfig, PVA Normative Types, PvMetadata, error handling     |
| **aura-net**      | PVAccess TCP client: session management, monitor subscriptions, binary codec  |
| **aura-ingest**   | Multi-threaded event processing: ScalarDelta fast path, heartbeat emission    |
| **aura-discover** | PV discovery: config polling, command publishing, PostgreSQL NOTIFY listener  |
| **aura-store**    | Database layer: 5 typed writers, binary COPY pipeline, migrations, query tier |
| **binary**        | Orchestration: startup, lifecycle handlers, adaptive tuning, shutdown         |

### Data Flow (Hot Path)

```
IOC (pvxs TCP)
  │
  ▼
PvaDriver ─ MonitorBus (N channels) ──▶ Ingest Thread 0..N
  │                                           │
  │  session multiplexing                     │ ScalarDelta decode
  │  automatic reconnect                      │ heartbeat injection
  │                                           │ metadata extraction
  │                                           ▼
  │                                    SharedBuffer (per-thread)
  │                                           │
  │                                           ▼
  │                                      store_loop (single task)
  │                                           │
  │                                     ┌─────┴─────┐
  │                                     ▼           ▼
  │                              take_flush    background
  │                              _bundle()     COPY task
  │                                     │           │
  │                                     ▼           ▼
  └── bounded backpressure ──────▶ TimescaleDB (hypertables)
      (drops counted at each stage)
```

## Performance

Every performance figure quoted for AURA-EPICS is produced by the
reproducible benchmark harness in [`bench/`](bench/README.md) and published
with its full manifest (git revision, image tag, host, parameters) — in
`bench/published/`. Numbers without a manifest in that directory are design
targets, not measurements.

| Claim                                             | How it is measured                                                                          |
|---------------------------------------------------|---------------------------------------------------------------------------------------------|
| Sustained throughput (~1M events/s design target) | `aura-bench throughput --rate 1000000 --pvs 100000 --duration-secs 3600 --pg-tuned --paper` |
| Saturation point and bottleneck                   | ramp mode, with bench-process AND PostgreSQL-container CPU columns                          |
| PV count ⊥ event rate                             | same rate at 1 / 10k / 1M / 4M PVs                                                          |
| Zero loss under PostgreSQL SIGKILL                | `aura-bench durability` (conservation equation, exit non-zero unless ZERO-LOSS)             |

Run the whole protocol with `bash bench/scripts/campaign.sh` on the
measurement server; the median run of each configuration lands in
`bench/published/INDEX.md` (median + spread).

### Storage Efficiency

TimescaleDB compression (gorilla + delta-of-delta + LZ4), with hourly chunks
rolled up into 24 h compressed chunks (see `migrations/007_compression.sql`):

| Data Type                             | Raw (payload)                            | Compressed             | Typical Ratio |
|---------------------------------------|------------------------------------------|------------------------|---------------|
| Scalar (f64 + alarm metadata)         | 26 bytes/row (~40 B with tuple overhead) | ~1–2 bytes/sample      | **~15–40×**   |
| Array (destructured, element-per-row) | 8 bytes/element                          | ~0.6–1.1 bytes/element | **~7–13×**    |
| String                                | variable                                 | LZ4                    | **~3×**       |

Ratios are workload-dependent (signal smoothness drives gorilla efficiency);
measure on your own data with `SELECT * FROM
hypertable_compression_stats('samples');` after a few hours of ingestion.
The 52-byte figure sometimes seen in the code is the binary **COPY wire
format** per row, not the on-disk size.

## Quickstart

### Prerequisites

- Rust 1.95.0 (pinned by `rust-toolchain.toml`; rustup installs it automatically on first `cargo` invocation)
- Docker with Docker Compose
- One or more EPICS IOCs with PVAccess enabled (pvxs)

### 1. Clone and build

```bash
git clone https://github.com/clemkraw/aura-epics.git
cd aura-epics
cargo build --release
```

### 2. Start infrastructure

```bash
cp .env.example .env    # defaults match config/aura.toml (user aura / db aura) — edit both together if you change them
docker compose -f deploy/docker-compose.yml --env-file .env up -d
```

This starts TimescaleDB (with gorilla compression) and Redis (used by the discovery orchestrator). The database schema
is applied automatically on first run via 15 embedded SQL migrations.

### 3. Configure

```bash
cp config/aura.toml.example config/aura.toml
```

Edit `config/aura.toml` with your IOC addresses (see [Configuration](#configuration)).

### 4. Run

```bash
./target/release/aura --config config/aura.toml
```

### 5. Add PVs to archive

```sql
-- PVs are picked up within 30s (polling) or instantly (NOTIFY)
INSERT INTO pv_config (pv_name)
VALUES ('CRYO:SECTOR1:TEMP'),
       ('MAG:DIPOLE:CURRENT'),
       ('VAC:CHAMBER:PRESSURE');

-- Hot-add a new IOC (no restart required)
INSERT INTO ioc_config (address)
VALUES ('10.0.1.5:5075');
```

## Configuration

### `aura.toml`

```toml
[redis]
url = "redis://127.0.0.1:6379"

[database]
url = "postgresql://aura:aura@localhost/aura"
max_connections = 5

[discover]
name_servers = ["192.0.2.10:5075"]
config_poll_interval_s = 30

[ingest]
default_heartbeat_s = 0.0   # 0 = heartbeat disabled (matches config/aura.toml.example); set e.g. 60.0 to force a row per PV at least every 60 s. Per-PV override: pv_config.heartbeat_s

[store]
batch_size = 200000
flush_interval_ms = 100

[telemetry]
log_level = "info"
log_format = "pretty"
```

### Hardware Adaptation

The archiver auto-detects hardware at startup and scales accordingly:

| Parameter        | Formula                                   | 8C/16T, 32 GB |
|------------------|-------------------------------------------|---------------|
| Ingest threads   | `logical_cpus / 4`                        | 4             |
| COPY connections | `logical_cpus / 2`                        | 8             |
| Buffer capacity  | `2% RAM / 32 bytes` (clamped 2M–20M)      | 20M rows      |
| Chunk interval   | `(shared_buffers/2) / (rate × 100 bytes)` | ~2 hours      |

## Database Schema

### 15 Embedded Migrations

The schema is versioned and applied automatically at startup. All migrations are idempotent (`IF NOT EXISTS`,
`CREATE OR REPLACE`).

```
001  extensions             TimescaleDB + pg_stat_statements
002  pv_lookup              pv_id ↔ pv_name normalization
003  pv_config              archiving config (heartbeat_s: NULL=default, 0=off, >0=override)
004  pv_metadata            PVA Normative Type metadata (units, alarms, enums)
005  samples                scalar + string hypertables (1-hour chunks)
006  samples_typed          per-NT-type hypertables (image, JSON, array)
007  compression            gorilla + delta-of-delta policies
008  retention              tiered retention (raw → downsampled → purge)
009  continuous_aggs        hourly and daily materialized views
010  alert_log              system alert history
011  array_destructured     element-per-row array tables
012  destructure_json       element-per-row JSON tables
013  ioc_config             IOC server registry with NOTIFY trigger
014  pv_events_status       lifecycle events + real-time PV status
015  batch_notify           statement-level NOTIFY on pv_config changes
```

### Query Tier Selection

The query layer automatically selects the optimal data source:

| Time Range     | Source                                 | Resolution                                                                                                                                               |
|----------------|----------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| span < 6 hours | `samples`                              | Full (raw) — only within the 90-day raw retention; older windows must use the aggregate tiers (current selector keys on span, not age: known limitation) |
| 6h – 7 days    | `samples_hourly` (real-time aggregate) | 1-hour buckets                                                                                                                                           |
| > 7 days       | `samples_daily` (real-time aggregate)  | 1-day buckets                                                                                                                                            |

Raw data is kept **90 days, lossless** (full precision, every point, compressed
~1-2 B/sample with 24 h chunk roll-up). Beyond 90 days: `samples_hourly` covers
2 years, `samples_daily` is kept forever. Both aggregates are real-time
(`materialized_only = false`), so they are complete up to `now()`.

## Normative Types

AURA natively archives the 11 archivable EPICS 7 Normative Types plus custom structures (NTURI, an RPC request type, and
NTAttribute, which only appears as a component of NTNDArray, are intentionally excluded as they do not exist as
standalone archivable PVs):

| NT             | Description                    | Storage                            |
|----------------|--------------------------------|------------------------------------|
| NTScalar       | Single numeric or string value | `samples` / `samples_string`       |
| NTEnum         | Enumerated value with choices  | `samples` (index as f64)           |
| NTScalarArray  | Waveform / array of values     | `samples_array_num` (destructured) |
| NTMatrix       | 2D array with dimensions       | `samples_array_num` (destructured) |
| NTNDArray      | Camera/detector images         | `samples_image` (BYTEA)            |
| NTHistogram    | Binned distribution            | `samples_hist` (destructured)      |
| NTContinuum    | Multi-trace waveform           | `samples_cont` (destructured)      |
| NTNameValue    | Key-value pairs                | `samples_nv` (destructured)        |
| NTTable        | Columnar data                  | `samples_table` (JSONB)            |
| NTMultiChannel | Multi-PV snapshot              | `samples_mch` (destructured)       |
| NTAggregate    | Pre-computed statistics        | `samples` (mean value)             |
| NTUnion        | Runtime-typed value            | `samples_custom` (JSONB)           |
| Custom         | Non-standard structures        | `samples_custom` (JSONB)           |

## Project Context

AURA-EPICS was originally initiated at [IJCLab](https://www.ijclab.in2p3.fr/) (Laboratoire de Physique des 2 Infinis
Irène
Joliot-Curie), a joint laboratory of CNRS/IN2P3 and Université Paris-Saclay, during an internship focused on the control
and data acquisition
needs of the [PERLE](https://perle-web.ijclab.in2p3.fr/) particle accelerator project (Powerful Energy Recovery Linac for
Experiments). Development has since continued
beyond this initial framework.

**Author:** Clément Krawiec (clemkraw)

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) before submitting a pull request.

## License

AURA-EPICS is licensed under the [MIT License](LICENSE).