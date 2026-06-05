```
aura-epics/
│
│   # ─────────────────────────────────────────────────────────────────
│   # WORKSPACE ROOT
│   # ─────────────────────────────────────────────────────────────────
│
├── Cargo.toml
├── Cargo.lock
├── rust-toolchain.toml
├── .cargo/
│   └── config.toml
│
├── LICENSE
├── README.md
├── CONTRIBUTING.md
├── CHANGELOG.md
├── SECURITY.md
├── .gitignore
│
│   # ─────────────────────────────────────────────────────────────────
│   # CONFIGURATION
│   # ─────────────────────────────────────────────────────────────────
│
├── config/
│   ├── aura.toml
│   └── examples/
│       ├── aura.dev.toml
│       └── aura.prod.toml
│
│   # ─────────────────────────────────────────────────────────────────
│   # CRATES
│   # ─────────────────────────────────────────────────────────────────
│
├── crates/
│   │
│   │   # ─────────────────────────────────────────────────────────────
│   │   # aura-core                          [DONE] 19 files, 2 551 lines, 43 tests
│   │   # Shared types, config, errors, telemetry, Prometheus setup.
│   │   # Every other crate depends on this. Zero business logic.
│   │   # ─────────────────────────────────────────────────────────────
│   │
│   ├── aura-core/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs                  # Crate root, module declarations, re-exports
│   │       ├── config.rs              # AuraConfig (deserialized from TOML, 9 sections)
│   │       ├── sample.rs             # PvUpdate (pipeline data unit), FilterDecision, StoreReason
│   │       ├── metadata.rs           # PvMetadata (auto-captured from first PVA monitor)
│   │       ├── pv.rs                 # PvConfig, PvStatus, IocState, IocInfo
│   │       ├── error.rs              # AuraError (10 variants), AuraResult
│   │       ├── telemetry.rs          # Tracing init (JSON prod / pretty dev)
│   │       └── pva/                  # Complete EPICS 7 PVAccess type system
│   │           ├── mod.rs            # Module root + flat re-exports
│   │           ├── scalars.rs        # ScalarValue (12 types: bool->string), ScalarType
│   │           ├── arrays.rs         # ArrayValue (12 array types), as_f64_vec()
│   │           ├── alarm.rs          # Alarm, AlarmSeverity (5), AlarmStatus (8)
│   │           ├── time.rs           # TimeStamp (ns precision, chrono conversion)
│   │           ├── display.rs        # Display, DisplayForm (7), Control, ValueAlarm
│   │           ├── enums.rs          # EnumValue (index + choices)
│   │           ├── ndarray.rs        # Codec, Dimension, NdAttribute
│   │           ├── table.rs          # TableColumn, HistogramValue (short/int/long)
│   │           ├── union.rs          # UnionValue (scalar/array/structure)
│   │           ├── normative.rs      # 13 NT structs + NormativeType enum:
│   │           │                     #   NTScalar, NTEnum, NTScalarArray,
│   │           │                     #   NTMatrix, NTHistogram, NTContinuum,
│   │           │                     #   NTNameValue, NTTable, NTNDArray,
│   │           │                     #   NTMultiChannel, NTAggregate, NTUnion,
│   │           │                     #   CustomStructure
│   │           └── classify.rs       # PvDataType (13 variants) -> storage routing
│   │
│   │   # ─────────────────────────────────────────────────────────────
│   │   # aura-net                           [TODO]
│   │   # Pure Rust PVAccess (PVXS) protocol implementation.
│   │   # No dependency on EPICS Base or PVXS C++ libraries.
│   │   # Publishable as a standalone crate for the community.
│   │   # ─────────────────────────────────────────────────────────────
│   │
│   ├── aura-net/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── pva/
│   │       │   ├── mod.rs              # PVAccess module root
│   │       │   ├── protocol.rs         # PVA protocol constants and opcodes
│   │       │   │                       #   PVA_MAGIC, CMD_SEARCH, CMD_CREATE_CHANNEL,
│   │       │   │                       #   CMD_MONITOR, CMD_BEACON, etc.
│   │       │   │                       #   PVA default ports: TCP 5075, UDP 5076
│   │       │   ├── codec.rs            # PVA message framing and serialization
│   │       │   │                       #   PVA header (8 bytes) + PVField payload
│   │       │   │                       #   Handles segmented messages
│   │       │   ├── beacon.rs           # PVA UDP beacon listener (port 5076)
│   │       │   │                       #   Parses server beacons
│   │       │   │                       #   Emits PvaBeacon { guid, address, port, protocol }
│   │       │   ├── client.rs           # PVA TCP client
│   │       │   │                       #   Connection handshake (SET_ENDIAN, VALIDATE)
│   │       │   │                       #   SEARCH channels
│   │       │   │                       #   CREATE_CHANNEL
│   │       │   │                       #   MONITOR (subscribe with pipeline)
│   │       │   ├── monitor.rs          # PVA monitor subscription manager
│   │       │   │                       #   Manages N subscriptions per IOC
│   │       │   │                       #   Handles reconnection on disconnect
│   │       │   │                       #   Emits Sample via mpsc channel
│   │       │   └── pvfield.rs          # PVAccess type system
│   │       │                           #   pvDouble, pvString, pvInt, pvEnum
│   │       │                           #   NTScalar, NTEnum, NTTable
│   │       │                           #   Introspection / field description parsing
│   │       │                           #   Conversion to f64 for archiving
│   │       │
│   │       └── search.rs              # PVA name search (UDP broadcast + unicast)
│   │                                   #   Resolves PV name -> IOC address
│   │                                   #   Caches results with TTL
│   │
│   │   # ─────────────────────────────────────────────────────────────
│   │   # aura-discover                      [TODO]
│   │   # IOC discovery + PV registry management.
│   │   # Compares pv_config table (what SHOULD exist) with network
│   │   # beacons (what DOES exist). Assigns IOCs to ingest shards.
│   │   # Singleton service (1 instance).
│   │   # ─────────────────────────────────────────────────────────────
│   │
│   ├── aura-discover/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── discovery.rs            # Main orchestrator
│   │       │                           #   Spawns beacon_scanner + config_poller
│   │       │                           #   Reconciles network state vs DB state
│   │       │                           #   Publishes commands to Redis pub/sub:
│   │       │                           #     aura:cmd:subscribe {pv_name, shard_id}
│   │       │                           #     aura:cmd:unsubscribe {pv_name, shard_id}
│   │       │                           #     aura:cmd:update_epsilon {pv_name, epsilon}
│   │       │
│   │       ├── config_poller.rs        # Polls pv_config every 30s via aura-store
│   │       │                           #   Detects: new PVs, removed PVs,
│   │       │                           #   changed epsilon/heartbeat, re-sharding
│   │       │
│   │       ├── beacon_scanner.rs       # Listens PVA beacons via aura-net (UDP 5076)
│   │       │                           #   Maintains map: IOC GUID -> address + last_seen
│   │       │                           #   Detects unknown IOCs (not serving any
│   │       │                           #   PV in pv_config)
│   │       │
│   │       ├── liveness.rs             # IOC state machine:
│   │       │                           #   ONLINE -> SUSPECT (no beacon for 45s)
│   │       │                           #          -> OFFLINE (no beacon for 150s)
│   │       │                           #   Updates ioc_registry table
│   │       │                           #   Publishes DISCONNECTED events to Redis
│   │       │
│   │       ├── shard_assigner.rs       # Assigns IOCs to ingest shards
│   │       │                           #   Strategy: consistent hashing on IOC GUID
│   │       │                           #   Rebalances when shards join/leave
│   │       │
│   │       └── alerts.rs              # Alert generation -> alert_log table
│   │                                   #   - PV configured but not connected
│   │                                   #   - IOC detected but not in config
│   │                                   #   - IOC went offline
│   │                                   #   - Shard lost (ingest instance down)
│   │
│   │   # ─────────────────────────────────────────────────────────────
│   │   # aura-ingest                        [TODO]
│   │   # Network ingestion + Redis publishing.
│   │   # Subscribes to PVs via PVA monitor, pushes ALL raw samples
│   │   # to Redis Streams (no filtering at ingest).
│   │   # Horizontally scalable (N shards).
│   │   # ─────────────────────────────────────────────────────────────
│   │
│   ├── aura-ingest/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── orchestrator.rs         # Main loop for one shard
│   │       │                           #   Listens for commands on Redis pub/sub:
│   │       │                           #     aura:cmd:subscribe / unsubscribe / update
│   │       │                           #   Manages per-IOC subscriber tasks
│   │       │                           #   Registers shard heartbeat in Redis
│   │       │
│   │       ├── subscriber.rs           # Per-IOC PVA subscriber task
│   │       │                           #   Opens PVA connection via aura-net
│   │       │                           #   Subscribes to all assigned PVs
│   │       │                           #   Pushes raw samples to publisher
│   │       │                           #   Handles reconnection on IOC restart
│   │       │
│   │       ├── publisher.rs            # Redis Streams producer
│   │       │                           #   Batches samples (redis_batch_size)
│   │       │                           #   Flushes on batch full OR timer
│   │       │                           #   Falls back to WAL on Redis failure
│   │       │                           #   XADD with MAXLEN ~ for stream capping
│   │       │
│   │       ├── wal.rs                  # Write-Ahead Log (local disk)
│   │       │                           #   Append-only file of serialized samples
│   │       │                           #   If Redis is unreachable, samples go to WAL
│   │       │                           #   When Redis recovers, WAL is replayed
│   │       │                           #   WAL is truncated after successful replay
│   │       │
│   │       └── metrics.rs              # Per-PV Prometheus metrics
│   │                                   #   aura_samples_received_total{pv}
│   │                                   #   aura_samples_stored_total{pv}
│   │                                   #   aura_redis_xadd_duration_seconds
│   │                                   #   aura_wal_depth
│   │
│   │   # ─────────────────────────────────────────────────────────────
│   │   # aura-store                         [DONE] 21 files, 8 692 lines, 315 tests
│   │   #                                          12 SQL migrations, 794 lines
│   │   # Database access layer and filtering engine.
│   │   # ONLY crate that touches TimescaleDB.
│   │   # Consumes Redis Streams -> filter -> batch write -> TimescaleDB.
│   │   # Provides query functions for aura-api.
│   │   # Horizontally scalable (consumer group).
│   │   # ─────────────────────────────────────────────────────────────
│   │
│   ├── aura-store/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs                  # Module declarations + re-exports (60 lines)
│   │       ├── pool.rs                 # PgPool setup, health check, TimescaleDB verify (430 lines, 17 tests)
│   │       ├── migrations.rs           # Embedded migration runner, 12 SQL files (520 lines, 26 tests)
│   │       │
│   │       ├── consumer.rs             # Redis Streams consumer (717 lines, 23 tests)
│   │       │                           #   XREADGROUP: reads batch of N messages
│   │       │                           #   XACK batching after successful write
│   │       │                           #   XAUTOCLAIM crash recovery from dead consumers
│   │       │                           #   Backpressure via configurable batch size
│   │       │
│   │       ├── filter/                 # Epsilon-deadband + heartbeat filter engine
│   │       │   ├── mod.rs              # FilterEngine orchestrator (442 lines, 17 tests)
│   │       │   │                       #   Routes PvUpdate -> per-PV filter
│   │       │   │                       #   Auto-creates PvFilter on first sample
│   │       │   │                       #   Compression ratio tracking
│   │       │   │
│   │       │   ├── pv_filter.rs        # Per-PV state machine (719 lines, 27 tests)
│   │       │   │                       #   Priority: heartbeat > alarm_change > epsilon
│   │       │   │                       #   Returns FilterDecision with StoreReason
│   │       │   │                       #   Array L2 norm support
│   │       │   │
│   │       │   ├── calibrator.rs       # Auto-epsilon calibration (567 lines, 19 tests)
│   │       │   │                       #   Sliding window (VecDeque, size=128)
│   │       │   │                       #   epsilon = k * sigma_window
│   │       │   │                       #   Non-stationarity detection
│   │       │   │
│   │       │   └── stats.rs            # Welford online mean/variance/std (445 lines, 22 tests)
│   │       │                           #   Numerically stable single-pass algorithm
│   │       │                           #   merge() for parallel reduction
│   │       │
│   │       ├── writer/                 # Batch writers (industrial grade)
│   │       │   ├── mod.rs              # BatchWriter orchestrator (833 lines, 27 tests)
│   │       │   │                       #   Dispatch by PvDataType (13 variants)
│   │       │   │                       #   Parallel flush via tokio::try_join!
│   │       │   │                       #   Zero-copy image extraction (std::mem::take)
│   │       │   │                       #   Error-tracked JSON serde (no silent defaults)
│   │       │   │                       #   FlushReport, WriterStats with backpressure
│   │       │   │
│   │       │   ├── scalar.rs           # ScalarWriter (797 lines, 41 tests)
│   │       │   │                       #   COPY FROM STDIN >= 50 rows, UNNEST < 50
│   │       │   │                       #   ScalarRow: Copy trait (26 bytes, zero heap)
│   │       │   │                       #   NaN/Infinity handling in COPY text format
│   │       │   │                       #   Backpressure (64 MB), PushResult enum
│   │       │   │
│   │       │   ├── string.rs           # StringWriter (729 lines, 44 tests)
│   │       │   │                       #   COPY >= 50, UNNEST < 50
│   │       │   │                       #   escape_copy_text() for tab/newline/backslash
│   │       │   │                       #   Backpressure (32 MB), total_value_bytes metric
│   │       │   │
│   │       │   ├── array.rs            # ArrayWriter (796 lines, 43 tests)
│   │       │   │                       #   Transaction INSERT < 50, COPY >= 50
│   │       │   │                       #   Quoted "values" column (PG reserved word)
│   │       │   │                       #   mem_size() per row, backpressure (64 MB)
│   │       │   │
│   │       │   ├── json.rs             # JsonWriter (651 lines, 41 tests)
│   │       │   │                       #   6 target tables: samples_table, samples_custom,
│   │       │   │                       #     samples_namevalue, samples_histogram,
│   │       │   │                       #     samples_continuum, samples_multi
│   │       │   │                       #   UNNEST batch for Table/Custom (1 round-trip)
│   │       │   │                       #   Transaction INSERT for rest (jsonb_array_elements)
│   │       │   │                       #   estimate_json_size(): zero-alloc recursive walker
│   │       │   │                       #   Backpressure (32 MB), per-table write counters
│   │       │   │
│   │       │   ├── image.rs            # ImageWriter (706 lines, 38 tests)
│   │       │   │                       #   Transaction INSERT (BYTEA, no UNNEST)
│   │       │   │                       #   mem_size() = ROW_OVERHEAD + data.len() + codec.len()
│   │       │   │                       #   Compression ratio tracking (codec effectiveness)
│   │       │   │                       #   Backpressure (128 MB), PushResult enum
│   │       │   │
│   │       │   └── pv_cache.rs         # PvCache (662 lines, 34 tests)
│   │       │                           #   HashMap pv_name -> pv_id with DB fallback
│   │       │                           #   max_entries limit (500K, ~40 MB)
│   │       │                           #   Saturation tracking, UPSERT on miss
│   │       │                           #   warm() loads all entries at startup
│   │       │
│   │       ├── reader.rs               # Query functions for aura-api (465 lines, 17 tests)
│   │       │                           #   QueryTier auto-select: <6h raw, 6h-7d hourly, >7d daily
│   │       │                           #   query_raw(), query_aggregated(), list_pvs()
│   │       │
│   │       ├── pv_config.rs            # CRUD on pv_config table (298 lines, 4 tests)
│   │       │                           #   get_all_enabled(), get_changed_since(ts)
│   │       │                           #   insert/update/delete
│   │       │
│   │       ├── metadata.rs             # PV metadata storage (215 lines, 4 tests)
│   │       │                           #   StoredMetadata with plain types (sqlx-safe)
│   │       │
│   │       ├── alerts.rs               # Alert log read/write (354 lines, 13 tests)
│   │       │                           #   AlertLevel, AlertCategory enums
│   │       │                           #   AlertDao with time-range queries
│   │       │
│   │       ├── metrics.rs              # StoreMetrics snapshot + Timer (267 lines, 9 tests)
│   │       │
│   │       └── pipeline.rs             # Main processing loop (399 lines, 13 tests)
│   │                                   #   read -> filter -> dispatch -> flush -> ack
│   │                                   #   startup(): warm cache + claim pending
│   │                                   #   shutdown(): final flush
│   │                                   #   ProcessResult with throughput tracking
│   │
│   │   # ─────────────────────────────────────────────────────────────
│   │   # aura-api                           [TODO]
│   │   # REST API + Prometheus metrics endpoint.
│   │   # Stateless, horizontally scalable.
│   │   # ─────────────────────────────────────────────────────────────
│   │
│   └── aura-api/
│       ├── Cargo.toml
│       └── src/
│           ├── lib.rs
│           ├── server.rs               # Axum server, router, middleware (CORS, tracing)
│           │
│           ├── routes/
│           │   ├── mod.rs
│           │   ├── query.rs            # GET /api/v1/query
│           │   │                       #   ?pv=PERLE:Gun:Vacuum
│           │   │                       #   &start=2026-04-01T00:00:00Z
│           │   │                       #   &end=2026-04-02T00:00:00Z
│           │   │                       #   &aggregate=1h (optional)
│           │   │
│           │   ├── pvs.rs              # GET    /api/v1/pvs
│           │   │                       # POST   /api/v1/pvs
│           │   │                       # PUT    /api/v1/pvs/:name
│           │   │                       # DELETE /api/v1/pvs/:name
│           │   │
│           │   ├── iocs.rs             # GET /api/v1/iocs
│           │   ├── status.rs           # GET /api/v1/status
│           │   ├── metrics.rs          # GET /api/v1/metrics[/:pv]
│           │   └── alerts.rs           # GET /api/v1/alerts
│           │
│           ├── models.rs               # API response types (Serialize)
│           └── health.rs               # GET /healthz
│
│   # ─────────────────────────────────────────────────────────────────
│   # BINARY ENTRYPOINT
│   # ─────────────────────────────────────────────────────────────────
│
├── src/
│   ├── main.rs                         # CLI (clap):
│   │                                   #   aura discover   -- discovery service
│   │                                   #   aura ingest     -- ingestion shard
│   │                                   #   aura store      -- DB writer
│   │                                   #   aura api        -- REST API
│   │                                   #   aura all        -- monolith mode
│   │                                   #   aura migrate    -- run DB migrations
│   │                                   #   aura status     -- print system status
│   │
│   └── banner.rs                       # Startup banner
│
│   # ─────────────────────────────────────────────────────────────────
│   # DATABASE MIGRATIONS                [DONE] 12 files, 794 lines SQL
│   # ─────────────────────────────────────────────────────────────────
│
├── migrations/
│   ├── 001_extensions.sql              # TimescaleDB + pg_stat_statements
│   ├── 002_pv_lookup.sql               # pv_name -> pv_id dictionary
│   ├── 003_pv_config.sql               # Per-PV archiving configuration
│   ├── 004_pv_metadata.sql             # Units, description, limits
│   ├── 005_samples.sql                 # Scalar hypertable (space-partitioned, 4 chunks)
│   ├── 006_samples_typed.sql           # String, array, image, histogram, continuum,
│   │                                   #   namevalue, multi, table, custom hypertables
│   ├── 007_compression.sql             # Compression after 1 hour (all hypertables)
│   ├── 008_retention.sql               # 90 days raw, 7 days image, configurable
│   ├── 009_continuous_aggs.sql         # Hourly + daily min/avg/max materialized views
│   ├── 010_ioc_registry.sql            # IOC tracking (guid, address, state)
│   ├── 010_alert_log.sql               # Alert history
│   └── 012_ingest_registry.sql         # Ingest shard registration
│
│   # ─────────────────────────────────────────────────────────────────
│   # DEPLOYMENT
│   # ─────────────────────────────────────────────────────────────────
│
├── deploy/
│   ├── docker/
│   │   ├── Dockerfile                  # Multi-stage:
│   │   │                               #   FROM rust:1.78-slim AS builder
│   │   │                               #   cargo build --release
│   │   │                               #   FROM debian:bookworm-slim
│   │   │                               #   COPY aura binary (~30 MB image)
│   │   │
│   │   └── docker-compose.yml          # Services:
│   │                                   #   redis          (redis:7-alpine)
│   │                                   #   timescaledb    (timescale/timescaledb:latest-pg16)
│   │                                   #   aura-discover  (aura discover, host network)
│   │                                   #   aura-ingest-0  (aura ingest --shard 0)
│   │                                   #   aura-ingest-1  (aura ingest --shard 1)
│   │                                   #   aura-store-0   (aura store)
│   │                                   #   aura-store-1   (aura store)
│   │                                   #   aura-api       (aura api, port 8080)
│   │                                   #   grafana        (grafana:latest, port 3000)
│   │                                   #   prometheus     (prom/prometheus, port 9090)
│   │
│   ├── grafana/
│   │   ├── provisioning/
│   │   │   ├── datasources/
│   │   │   │   ├── timescaledb.yaml
│   │   │   │   └── prometheus.yaml
│   │   │   └── dashboards/
│   │   │       └── provider.yaml
│   │   └── dashboards/
│   │       ├── aura-overview.json      # System: PV count, msg/s, storage, lag
│   │       ├── aura-pv-explorer.json   # Single-PV: signal, epsilon, decisions
│   │       └── aura-infra.json         # Infra: Redis depth, COPY latency, CPU, mem
│   │
│   ├── prometheus/
│   │   └── prometheus.yml              # Scrape targets:
│   │                                   #   aura-ingest-*:9090/metrics
│   │                                   #   aura-store-*:9090/metrics
│   │                                   #   aura-api:9090/metrics
│   │                                   #   aura-discover:9090/metrics
│   │
│   └── systemd/
│       ├── aura-discover.service
│       ├── aura-ingest@.service        # Template unit: aura-ingest@0, @1, @2...
│       ├── aura-store@.service
│       └── aura-api.service
│
│   # ─────────────────────────────────────────────────────────────────
│   # TESTING & BENCHMARKS
│   # ─────────────────────────────────────────────────────────────────
│
├── tests/
│   ├── integration/
│   │   ├── test_ingest_filter.rs       # PVA sample -> filter -> Redis
│   │   ├── test_store_writer.rs        # Redis -> COPY -> TimescaleDB
│   │   ├── test_api_query.rs           # API query correctness
│   │   ├── test_discovery_lifecycle.rs # Config change -> subscribe/unsub
│   │   ├── test_wal_recovery.rs        # Redis down -> WAL -> Redis up -> replay
│   │   └── test_end_to_end.rs          # Simulated IOC -> API response
│   │
│   └── fixtures/
│       ├── signals/
│       │   ├── stable_cryo.csv
│       │   ├── quench_magnet.csv
│       │   ├── ramp_current.csv
│       │   └── noisy_vacuum.csv
│       └── ioc/
│           └── test.db
│
├── benches/
│   ├── filter_throughput.rs           # Samples/s through epsilon-filter
│   ├── filter_latency.rs              # p50/p99 of filter decision
│   ├── redis_roundtrip.rs             # XADD -> XREADGROUP latency
│   ├── copy_throughput.rs             # COPY batch write speed
│   └── end_to_end.rs                  # Full pipeline throughput
│
│   # ─────────────────────────────────────────────────────────────────
│   # TOOLS
│   # ─────────────────────────────────────────────────────────────────
│
├── tools/
│   ├── aura-sim/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       └── main.rs                 # PVA IOC simulator
│   │                                   #   Broadcasts PVA beacons
│   │                                   #   Serves mock monitor data
│   │                                   #   Usage: aura-sim --iocs 100 --pvs-per-ioc 500
│   │                                   #          --update-rate 10  (= 50k PVs @ 10 Hz)
│   │
│   └── analysis/
│       ├── compression_report.py       # Compare AURA vs raw archiving
│       ├── fidelity_check.py           # Signal reconstruction RMSE
│       └── plot_filtering.py           # Visualize epsilon decisions per PV
│
│   # ─────────────────────────────────────────────────────────────────
│   # DOCUMENTATION & PUBLICATION
│   # ─────────────────────────────────────────────────────────────────
│
├── docs/
│   ├── architecture.md
│   ├── deployment.md
│   ├── configuration.md
│   ├── api-reference.md
│   ├── pva-protocol.md                 # PVAccess implementation notes
│   ├── filtering-theory.md             # Epsilon-deadband + entropy math
│   └── images/
│       ├── architecture.svg
│       └── data-flow.svg
│
└── paper/
    ├── aura-icalepcs.tex
    ├── figures/
    ├── references.bib
    └── Makefile
```