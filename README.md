[![Crates.io Version](https://img.shields.io/crates/v/pg_dbmigrator)](https://crates.io/crates/pg_dbmigrator)
[![Crates.io Downloads (recent)](https://img.shields.io/crates/dr/pg_dbmigrator)](https://crates.io/crates/pg_dbmigrator)
[![Crates.io Total Downloads](https://img.shields.io/crates/d/pg_dbmigrator)](https://crates.io/crates/pg_dbmigrator)
[![docs.rs](https://img.shields.io/docsrs/pg_dbmigrator)](https://docs.rs/pg_dbmigrator)
[![codecov](https://codecov.io/gh/isdaniel/pg_dbmigrator/graph/badge.svg)](https://codecov.io/gh/isdaniel/pg_dbmigrator)

# pg_dbmigrator

A Rust library and CLI for migrating PostgreSQL databases between two
endpoints, a one-shot dump/restore for cold moves, and an online path that
keeps PostgreSQL's built-in logical replication apply worker pulling from
the source so the operator can cut over with near-zero downtime.

The online path issues `CREATE SUBSCRIPTION` on the target attached to a
slot we created with `EXPORT_SNAPSHOT` before `pg_dump` ran.

## Modes

| Mode      | Behaviour                                                                                                                                                                                                          |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `offline` | Run `pg_dump` against the source, then `pg_restore` against the target. One-shot copy.                                                                                                                             |
| `online`  | Create a logical replication slot with `EXPORT_SNAPSHOT`, take a snapshot-consistent `pg_dump`, `pg_restore` it, then start a streaming WAL apply from the slot's start LSN until the operator triggers cutover.   |
| `verify`  | Read-only. Compare per-table row counts between source and target and exit non-zero on any mismatch. No dump, no restore, no replication. See [Verify](#verify).                                                    |

### Online migration phases

```
Validate → SourceVacuum → PrepareSnapshot → Dump → Restore → Analyze → StreamApply → (Lag heartbeat …) → CaughtUp → Cutover → SourceCleanup → Complete
```

* `Validate` pre-flights the source (`wal_level = 'logical'`,
  `max_replication_slots > 0`, `max_wal_senders > 0`) and ensures the
  required publication exists — auto-creating it if missing (see
  [Publication lifecycle](#publication--replication-resource-lifecycle)
  below).
* `SourceVacuum` runs `VACUUM ANALYZE` on the source to reclaim dead tuples
  and refresh planner statistics before the dump. Skip with `--skip-source-vacuum`.
* `PrepareSnapshot` creates the replication slot first; `START_REPLICATION`
  is deferred until **after** the dump completes, so the exported snapshot
  remains valid for the dump.
* `Analyze` runs `ANALYZE` on the target after restore so the query planner
  has fresh statistics for the first application queries. Skip with `--skip-analyze`.
* During `StreamApply` the library polls
  `pg_current_wal_flush_lsn()` on the source every
  `--cutover-poll-secs` and emits a `Lag` progress event with
  `lag_bytes / source_lsn / received_lsn / applied_lsn`. This is the
  signal the customer watches to decide when to cut over.
* When the lag drops at or below `--lag-threshold-bytes` a one-shot
  `CaughtUp` event is emitted (“ready for cutover”).
* `SourceCleanup` (after cutover) drops auto-created publications and
  replication slots on the source — see the next section.

Online mode does **not** run an automatic row-count verification. Cutover
stops the apply worker but does not freeze source writes, so an automatic
`count(*)` compare would show spurious mismatches (readiness is signalled by
the lag heartbeat, not row counts). Verify manually once the source is
quiesced — see [Verify](#verify).

## Install

`pg_dbmigrator` shells out to `pg_dump` and `pg_restore`, so it needs a PostgreSQL client at least as new as your **source** server. The install script sets both up in one command.

### One-liner

```bash
curl -fsSL https://raw.githubusercontent.com/isdaniel/pg_dbmigrator/main/install.sh | sh
```

Installs a static binary to `~/.local/bin` (no root), then installs `postgresql-client` for your distro (needs root, via passwordless `sudo`/`doas`).

If you pass the connection strings, the installer asks the servers what they run — over the wire protocol, so this works before any client exists — and installs the client matching the **newer of source and target** rather than simply the newest available. That is the floor `pg_dump` needs, and stopping there avoids a newer `pg_restore` emitting settings an older target does not recognise. An existing `pg_dump` is left alone unless it is older than what the servers require.

```bash
curl -fsSL https://raw.githubusercontent.com/isdaniel/pg_dbmigrator/main/install.sh \
  | PG_DBMIGRATOR_SOURCE='postgres://…/src' \
    PG_DBMIGRATOR_TARGET='postgres://…/dst' sh
```

Supports Ubuntu, Debian, RHEL/Rocky/AlmaLinux, Amazon Linux 2023, Alpine, openSUSE/SLES and Fedora, on x86_64 and aarch64. On Debian/Ubuntu and RHEL-likes it pulls the client from [apt.postgresql.org][pgdg] / [yum.postgresql.org][pgdg] rather than the distro repos, whose clients are often too old to dump a modern server (Ubuntu 22.04 ships PG 14, RHEL 9 ships PG 13 — both refuse a PG 17 source).

Tunable with environment variables:

| Variable | Default | Purpose |
|---|---|---|
| `PG_DBMIGRATOR_VERSION` | latest | pin a release, e.g. `v0.3.0` |
| `PG_DBMIGRATOR_BIN_DIR` | `~/.local/bin` | where the binary goes |
| `PG_DBMIGRATOR_SKIP_DEPS` | `0` | `1` = do not touch `pg_dump`/`pg_restore` |
| `PG_DBMIGRATOR_SOURCE` | — | source URI, probed for its major version |
| `PG_DBMIGRATOR_TARGET` | — | target URI, probed for its major version |
| `PG_MAJOR` | newest (`18`) | force a client major, skipping the probe |
| `PG_DBMIGRATOR_BASE_URL` | GitHub Releases | internal mirror / air-gapped install |

### With cargo-binstall (prebuilt binary, no compile)

```bash
cargo binstall pg_dbmigrator
```

### From source

```bash
cargo install pg_dbmigrator
```

### Manual download

Grab a tarball and `checksums.txt` from the [releases page][releases], verify, and drop the binary anywhere on `$PATH`:

```bash
sha256sum -c checksums.txt
tar -xzf pg_dbmigrator-v0.3.0-x86_64-unknown-linux-musl.tar.gz
```

Binaries are statically linked against musl, so one build runs on any glibc or musl distro.

```bash
pg_dbmigrator --help
pg_dbmigrator --mode offline --source '…' --target '…' --jobs 4
```

[pgdg]: https://www.postgresql.org/download/
[releases]: https://github.com/isdaniel/pg_dbmigrator/releases

## CLI

### Offline

```bash
pg_dbmigrator \
    --mode offline \
    --source 'postgres://user:pw@src.example/db' \
    --target 'postgres://user:pw@dst.example/db' \
    --jobs 4 \
    --drop-target-first
```

By default, `VACUUM ANALYZE` runs on the source before `pg_dump` and
`ANALYZE` runs on the target after `pg_restore`. Disable with
`--skip-source-vacuum` / `--skip-analyze` if you manage maintenance
externally.

Before the dump runs, offline migrations also pre-flight the target role's
privileges and extension availability (source extensions must be installable
on the target), so a misconfigured target fails fast with a clear error
instead of stalling inside `pg_restore`.

### Large single tables (`--split-tables-larger-than`)

`pg_dump --jobs` parallelises across archive *entries*, and a table is one
entry — so a single 300 GB table dumps on one worker no matter how high
`--jobs` goes. Worse, when the migrator runs on a third host the rows cross
the network twice: once into the archive, once back out to the target.
(`pg_dump`'s compression does not help here; it happens on the migrator,
after the bytes have already arrived.)

```bash
pg_dbmigrator --mode offline \
    --source '…' --target '…' --jobs 8 \
    --split-tables-larger-than 50GB   # off unless you pass it
```

Tables at or above the threshold keep their `CREATE TABLE` in the archive
but have their rows excluded (`pg_dump --exclude-table-data`) and streamed
straight from source to target instead, as `--split-max-parts` concurrent
`ctid`-range `COPY` streams (default 4). Every stream reads the same
exported snapshot, so the result is consistent with the rest of the dump.

Because the split planner reads the source catalog directly rather than
reimplementing `pg_dump`'s pattern grammar, and because an exported
snapshot does not outlive the process that took it, the option **refuses**
rather than silently degrading when combined with:

| Combination | Why it is refused |
|---|---|
| `--schema` / `--table` / `--exclude-schema` / `--exclude-table` | The planner and `pg_dump` could disagree about which tables are in scope, excluding a table's data from the archive while not copying it directly (or the reverse). |
| `--resume` | A resumed run cannot re-acquire the snapshot the archive was taken at, and re-deriving the plan could skip a table that has since dropped below the threshold — after its data was already excluded from the archive. |
| `--no-split-sections` | The copy has to land between the data and post-data sections. After an all-in-one restore the target already has indexes, foreign keys and triggers, and `TRUNCATE` fails outright on an FK-referenced table. |
| `--dump-scope schema-only` | There is no data to move. |

The copy also refuses to truncate a target table that already holds rows
unless `--drop-target-first` is passed, so a mistargeted run stops instead
of overwriting data it did not create.

### Online

On the source, before starting:

```sql
ALTER SYSTEM SET wal_level = 'logical';   -- requires restart
```

The publication is auto-created by the migrator if it does not already
exist (default `FOR ALL TABLES`). If you prefer to create it manually —
e.g. to publish only specific tables — run:

```sql
CREATE PUBLICATION pg_dbmigrator_pub FOR TABLE my_schema.t1, my_schema.t2;
```

pass `--no-auto-create-publication` so the migrator uses the existing
one and does not attempt to create or drop it.

```bash
pg_dbmigrator \
    --mode online \
    --source 'postgres://user:pw@src/db' \
    --target 'postgres://user:pw@dst/db' \
    --slot-name pg_dbmigrator_slot \
    --publication pg_dbmigrator_pub \
    --subscription-name pg_dbmigrator_sub \
    --jobs 4 \
    --lag-threshold-bytes 8192 \
    --cutover-poll-secs 5
```

Before the dump runs, the migrator pre-flights the source:
`wal_level = 'logical'`, `max_replication_slots > 0`,
`max_wal_senders > 0`, and extension availability (source extensions must be
installable on the target). A misconfigured source fails fast with a clear
error instead of stalling later inside `CREATE_REPLICATION_SLOT`.

At cutover, the migrator runs `setval(...)` on every sequence in the
included schemas so the target picks up where the source left off —
otherwise the first `INSERT` after cutover would collide with rows the
subscription replicated. Disable with `--no-sequence-sync` if your
target role lacks privileges for `setval` on those sequences.

### Filtering

Use `--exclude-schema` and `--exclude-table` to omit large or transient
objects from the dump. Both flags accept multiple values.

```bash
pg_dbmigrator --mode offline \
    --source ... --target ... \
    --exclude-schema audit \
    --exclude-table public.large_log
```

### Verify

Compare per-table row counts between source and target.

In **offline** mode this runs automatically after restore; a mismatch is
logged as a warning by default. Use `--verify strict` to make a mismatch a
hard error (non-zero exit) for CI, or `--verify off` to skip the step.

In **online** mode verification is **manual**, not automatic. Online cutover
stops the apply worker but does not freeze source writes, so an automatic
row-count compare would almost always show a spurious mismatch — readiness is
signalled by the lag heartbeat (`CaughtUp`), not by row counts. Once the
operator has quiesced source writes and lag has drained, run verification
explicitly with `--mode verify` (below).

Standalone (read-only, no dump/restore) — always exits non-zero on mismatch:

```bash
pg_dbmigrator --mode verify \
    --source 'postgres://user:pw@src/db' \
    --target 'postgres://user:pw@dst/db' \
    --schema app
```

Honours `--schema` / `--table` / `--exclude-schema` / `--exclude-table` so the
verified object set matches what you migrated.

### Other flags

| Flag | Mode | Purpose |
|---|---|---|
| `--json` | all | Emit machine-readable NDJSON progress events to stdout, one `ProgressEvent` per line. Human-readable logs stay on stderr. Pair with `RUST_LOG=warn,pg_dbmigrator=warn` for clean piping. |
| `--verbose` | offline, online | Run the source `VACUUM ANALYZE` and the target `ANALYZE` as `VERBOSE`, so PostgreSQL reports per-table progress. It does **not** change the migrator's own log level — use `RUST_LOG` for that. |
| `--dump-scope <all\|schema-only\|data-only>` | offline, online | What to dump. Default `all`. |
| `--dump-path <PATH>` | offline, online | Pin the dump archive path. Defaults to a unique path inside `$TMPDIR`. Required with `--resume`. |
| `--resume` | offline, online | Resume a previous run: read `<dump_path>.resume.json`, check the surrounding config still matches, and skip every stage already marked complete. Requires `--dump-path`. |
| `--resume-file <PATH>` | offline, online | Override the resume token path. Defaults to `<dump_path>.resume.json`. |
| `--split-sections` | offline, online | Restore pre-data, data and post-data as separate passes. Enabled by default; disable with `--no-split-sections`. |
| `--no-table-access-method` | offline, online | Pass `--no-table-access-method` to `pg_dump` (PG 15+), omitting `USING <access_method>` from CREATE TABLE. Use when the target lacks the source's custom table AMs. |
| `--force-clean` | online | Best-effort drop of a leftover subscription on the target and replication slot on the source from a previous crashed run, before starting. Use when a run died after `CREATE SUBSCRIPTION` and the next would fail with "already exists". |
| `--subscription-source <URI>` | online | Source URI written into `CREATE SUBSCRIPTION ... CONNECTION`. Set when the target's apply worker reaches the source at a different address than the migrator does (Docker service name vs. host loopback). Defaults to `--source`. |
| `--max-runtime-seconds <N>` | online | Stop the streaming apply phase after N seconds. |
| `--cutover-fast-poll-ms <MS>` | online | Tighter poll cadence once `lag_bytes <= --lag-threshold-bytes`. Default 1000. |
| `--protocol-version <N>` | online | pgoutput protocol version, validated to `1..=4` (default 2) but currently inert — neither `CREATE_REPLICATION_SLOT` nor `CREATE SUBSCRIPTION` carries a protocol version. |
| `--print-client-major` | — | Connect to source and target, print the PostgreSQL client major version that fits both (the newer of the two), and exit without migrating. `install.sh` uses this to pick a `postgresql-client` package. |

## Publication / replication resource lifecycle

The migrator fully manages the lifecycle of the replication resources it
creates, so the operator does not need to run manual cleanup SQL after a
successful cutover.

| Resource | Created by | Cleaned up at cutover | Override |
|---|---|---|---|
| Publication on source | Auto-created if missing (default) | Dropped only if it was auto-created | `--no-auto-create-publication` |
| Replication slot on source | Always created by the migrator | Dropped by default | `--keep-slot` |
| Subscription on target | Always created by the migrator | Dropped by default | `--keep-subscription` |

**Auto-create publication**: By default, the migrator checks whether the
named publication (`--publication`, default `pg_dbmigrator_pub`) exists on
the source. If it does not, the migrator creates it as `FOR ALL TABLES`
(or scoped to `--table` / `--schema` if specified). Auto-created
publications are tracked and dropped on the source after a successful
cutover. Pre-existing publications are never dropped.

**Slot cleanup**: After cutover, the replication slot on the source is no
longer needed. By default the migrator drops it. Pass `--keep-slot` if
you need to inspect the slot post-migration or if another consumer
shares it.

All cleanup steps are best-effort — failures are logged as warnings but
do not abort the migration.

## Cutover (online mode)

Cutover is driven by `SIGINT` (Ctrl+C). The CLI prints a periodic `Lag` heartbeat after the dump completes, so the operator has a continuous bytes-behind read-out:

```
INFO stage=Lag replication lag 4096 bytes (source LSN …, received LSN …, applied LSN …)
INFO stage=Lag replication lag 1024 bytes (…)
INFO stage=CaughtUp target caught up with source (lag 512 bytes) — ready for cutover
```

When the customer is satisfied with the lag, they press **Ctrl+C** once:

* The signal handler calls `CutoverHandle::request()`.
* The streaming apply loop notices the request on its next poll, flushes
  the last LSN feedback to the source, emits a `Cutover` event, and
  returns.
* The migrator syncs sequences, cleans up replication resources (publication,
  slot, subscription), and returns with
  `MigrationOutcome::cutover_triggered() == true`. The process exits
  cleanly. Application traffic can now be switched to the target.
* A second Ctrl+C is treated as an abort (escape hatch — only use it if
  the graceful path is stuck).

Cutover is always operator-driven; `--lag-threshold-bytes` is purely advisory and only controls when the one-shot `CaughtUp` “ready for cutover” event fires.

For online migrations, hold on to `migrator.cutover_handle()` and call `request()` from your own signal handler / RPC endpoint when the operator is ready to cut over. See [`examples/online_migration`](examples/online_migration) for a complete program that wires Ctrl+C to the cutover handle.

## Performance defaults

The CLI ships with sensible defaults tuned for migration speed. Override
only when you have a specific reason.

| Default | Flag to override | Effect |
|---|---|---|
| Split-section restore | `--no-split-sections` | Bulk COPY without index maintenance, then rebuild indexes in parallel. 30-60% faster on index-heavy schemas. |
| `pg_dump`'s own default compression (no `--compress` passed) | `--dump-compress <spec>` | The CLI passes no `--compress`, so `pg_dump` uses its format's own default. Set `lz4:1` for negligible CPU and a 3-5x smaller archive, or `zstd:3` for the best ratio. Needs `pg_dump` 16+; older clients take a bare digit `0`-`9`. Library users get `lz4:1` by default, from both `MigrationConfig::default()` and serde deserialization. |
| `--no-sync` on dump | `--keep-sync` | Skip fsync on transient dump files. |
| `--no-comments` | _(not exposed)_ | Omit COMMENT ON statements from dump. |
| `--no-security-labels` | _(not exposed)_ | Omit SE-Linux security labels from dump. |
| `--no-publications` | `--keep-publications` | Don't dump publication definitions to the target. |
| `--no-subscriptions` | `--keep-subscriptions` | Don't dump subscription definitions to the target. |
| Auto-detect `--jobs` | `--jobs N` | Clamps to `[1, 8]` based on host CPU count. |
| Pre-dump `VACUUM ANALYZE` | `--skip-source-vacuum` | Clean heap pages + fresh stats before dump. |
| Post-restore `ANALYZE` | `--skip-analyze` | Fresh planner stats on target immediately after restore. |
| Row-count verify | `--verify <off\|warn\|strict>` | Governs the **offline** auto verify step (per-table `count(*)`, source vs target, after restore). `off` skips it; `warn` (default) logs mismatches and continues; `strict` makes a mismatch a hard (non-zero exit) error. Online mode has no auto verify — verify manually via `--mode verify`. |

## Benchmark

See [BENCHMARK.md](BENCHMARK.md) for migration performance results across 10 GB -- 200 GB datasets (PG 16 -> PG 18, 8 parallel jobs, zstd compression).

## Known limitations

* **Object ownership and ACLs are not migrated.** The restore runs with
  `--no-owner --no-acl`, so every restored object ends up owned by the
  role in `--target` and all `GRANT`s are dropped. Roles themselves are
  not copied either (there is no `pg_dumpall --globals-only` step). This
  is deliberate — the target's permission model is assumed to be managed
  separately — but it means a migrated database is *not* a permissions
  replica of the source. Re-apply ownership and grants after cutover.
* **Online mode requires `idle_in_transaction_session_timeout = 0` on the
  source.** The connection that exports the snapshot stays idle inside a
  transaction for the entire `pg_dump`; a non-zero timeout terminates it
  part-way through and fails the migration. Preflight refuses to start
  when the setting is non-zero. Offline mode is unaffected.
* Apply runs through PostgreSQL's own logical replication apply worker
  (`CREATE SUBSCRIPTION`), not an in-process decoder, so type fidelity
  matches native logical replication. The flip side is that no
  per-column transform can be applied during replication.
* DDL changes are not migrated automatically — refresh the publication
  and restart the migration if the schema changes during the run.
* Extensions whose internal state cannot be re-created on the target
  (Azure-reserved extensions, pg_cron metadata, ...) may cause
  `pg_restore` to exit with code 1. Pass `--allow-restore-errors` to
  treat that as a non-fatal warning when user data was restored
  successfully.
* Sequence sync at cutover requires the target role to have permission
  to call `setval()` on the destination sequences. Per-sequence
  failures are logged but do not abort cutover — inspect the warnings
  and re-`setval` manually if needed, or pre-grant `USAGE` on the
  sequences before the migration.
