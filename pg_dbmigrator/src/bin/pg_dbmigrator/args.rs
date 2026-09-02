//! Command-line interface argument definitions.

use std::path::PathBuf;

use clap::{Parser, ValueEnum};

use pg_dbmigrator::config::{default_jobs, DumpScope};
use pg_dbmigrator::{
    CutoverConfig, EndpointConfig, MigrationConfig, MigrationMode, OnlineOptions,
    ReplicationApplyConfig, VerifyMode,
};

/// pg_dbmigrator — PostgreSQL → PostgreSQL database migration tool.
#[derive(Debug, Parser)]
#[command(name = "pg_dbmigrator", version, about)]
pub struct Cli {
    /// Migration mode.
    #[arg(long, value_enum, default_value_t = ModeArg::Offline)]
    pub mode: ModeArg,

    /// Source connection string (libpq URI).
    #[arg(long, env = "PG_DBMIGRATOR_SOURCE")]
    pub source: String,

    /// Target connection string (libpq URI).
    #[arg(long, env = "PG_DBMIGRATOR_TARGET")]
    pub target: String,

    /// What to dump (schema, data, or all).
    #[arg(long, value_enum, default_value_t = DumpScopeArg::All)]
    pub dump_scope: DumpScopeArg,

    /// Drop and recreate target schema before restoring.
    #[arg(long)]
    pub drop_target_first: bool,

    /// Number of parallel dump/restore jobs. Defaults to the host's logical CPU count, clamped to the range [1, 8].
    #[arg(long, default_value_t = default_jobs())]
    pub jobs: usize,

    /// Repeatable: schemas to migrate (default: all).
    #[arg(long = "schema")]
    pub schemas: Vec<String>,

    /// Repeatable: tables to migrate, formatted as `schema.table`.
    #[arg(long = "table")]
    pub tables: Vec<String>,

    /// Repeatable: schemas to exclude from the migration. Useful when
    /// the source has tenant / audit / vendor-managed schemas that
    /// should not be replicated. Forwarded to `pg_dump --exclude-schema=`.
    #[arg(long = "exclude-schema")]
    pub exclude_schemas: Vec<String>,

    /// Repeatable: tables to exclude from the migration, formatted as
    /// `schema.table`. Forwarded to `pg_dump --exclude-table=`.
    #[arg(long = "exclude-table")]
    pub exclude_tables: Vec<String>,

    /// Replication slot name (online mode only).
    #[arg(long, default_value = "pg_dbmigrator_slot")]
    pub slot_name: String,

    /// Publication name (online mode only).
    #[arg(long, default_value = "pg_dbmigrator_pub")]
    pub publication: String,

    /// Subscription name created on the target. Online mode only.
    #[arg(long, default_value = "pg_dbmigrator_sub")]
    pub subscription_name: String,

    /// Override for the source URI written into
    /// `CREATE SUBSCRIPTION ... CONNECTION '<…>'`. Set this when the
    /// target's apply worker reaches the source via a different address than
    /// the migrator (e.g. Docker service name vs. host loopback). Defaults
    /// to `--source` when unset. Online mode only.
    #[arg(long)]
    pub subscription_source: Option<String>,

    /// Keep the subscription on the target after cutover (default: drop it).
    /// Online mode only.
    #[arg(long)]
    pub keep_subscription: bool,

    /// Best-effort cleanup of any leftover subscription on the target and
    /// replication slot on the source from a previous (crashed) run before
    /// starting. Use this when a previous run died after `CREATE
    /// SUBSCRIPTION` and the next run would otherwise fail with
    /// "subscription already exists" / "slot already exists". Online mode
    /// only.
    #[arg(long)]
    pub force_clean: bool,

    /// Disable the post-cutover sequence sync (online mode only).
    /// PostgreSQL logical replication does not replay `nextval()`, so by
    /// default the migrator runs `setval(...)` on every target sequence
    /// at cutover so the first post-cutover INSERT does not collide
    /// with a replicated row. Disable only if you have your own
    /// out-of-band sequence sync (e.g. UUID PKs).
    #[arg(long)]
    pub no_sequence_sync: bool,

    /// Disable auto-creation of the publication on the source. By default the migrator creates the publication automatically if it does not exist. Pass this flag to require the operator to create it manually before running the migration. Online mode only.
    #[arg(long)]
    pub no_auto_create_publication: bool,

    /// Keep the replication slot on the source after cutover (default: drop it). Online mode only.
    #[arg(long)]
    pub keep_slot: bool,

    /// pgoutput protocol version.
    #[arg(long, default_value_t = 2)]
    pub protocol_version: u32,

    /// Stop the streaming apply phase after N seconds (online mode only).
    #[arg(long)]
    pub max_runtime_seconds: Option<u64>,

    /// Advisory threshold (WAL bytes). When `lag_bytes` first drops at or
    /// below this value the apply loop emits a one-shot `CaughtUp`
    /// ("ready for cutover") event — purely informational so the operator
    /// knows the cheapest moment to cut over. The bytes-behind `Lag`
    /// heartbeat is still printed every `--cutover-poll-secs` regardless.
    /// Cutover itself is always operator-driven via Ctrl+C. Online mode
    /// only.
    #[arg(long, default_value_t = 8 * 1024)]
    pub lag_threshold_bytes: u64,

    /// How often to poll the source's current WAL LSN, in seconds. Online
    /// mode only.
    #[arg(long, default_value_t = 5)]
    pub cutover_poll_secs: u64,

    /// Tighter poll cadence (milliseconds) used once `lag_bytes <=
    /// --lag-threshold-bytes`. Default 1000 ms. Online mode only.
    #[arg(long, default_value_t = 1000)]
    pub cutover_fast_poll_ms: u64,

    /// Pin the dump archive output path. By default a unique path inside
    /// `$TMPDIR` is used. Required when `--resume` is set.
    #[arg(long)]
    pub dump_path: Option<PathBuf>,

    /// Resume a previous run. The orchestrator reads
    /// `<dump_path>.resume.json` (or `--resume-file`), validates the
    /// surrounding config still matches, and skips every stage already
    /// marked complete. Requires `--dump-path` so successive runs target
    /// the same archive.
    #[arg(long)]
    pub resume: bool,

    /// Override path for the resume token file. Defaults to
    /// `<dump_path>.resume.json`.
    #[arg(long)]
    pub resume_file: Option<PathBuf>,

    /// Treat `pg_restore` exit 1 (`errors ignored on restore: N`) as a
    /// non-fatal warning. Use for cross-server migrations where the source
    /// has installed extensions whose state cannot be re-created on the
    /// target (Azure-reserved extensions, pg_cron metadata tables, etc.).
    /// User data still restores; only extension internal state fails.
    #[arg(long)]
    pub allow_restore_errors: bool,

    /// Pass `--publications` (i.e. *do* dump publications) to `pg_dump`.
    /// Default behaviour is `--no-publications` since publications are
    /// migration scaffolding that produce noisy `wal_level is insufficient`
    /// warnings on the target.
    #[arg(long)]
    pub keep_publications: bool,

    /// Pass `--subscriptions` (i.e. *do* dump subscriptions) to `pg_dump`.
    /// Default behaviour is `--no-subscriptions` to avoid carrying over
    /// subscriptions that point at the previous upstream.
    #[arg(long)]
    pub keep_subscriptions: bool,

    /// Restore in three phases — `pre-data` → `data` → `post-data` —
    /// instead of one all-in-one `pg_restore` call. Skips index
    /// maintenance during the bulk `COPY` phase and rebuilds every index
    /// in parallel against fully-loaded tables. Typically 30–60 % faster
    /// on schemas with many secondary indexes; requires a directory- or
    /// custom-format dump. Enabled by default.
    #[arg(long, default_value_t = true)]
    pub split_sections: bool,

    /// Disable split-section restore (use a single all-in-one pg_restore
    /// call). Overrides the default `--split-sections` behaviour.
    #[arg(long)]
    pub no_split_sections: bool,

    /// Copy tables at or above this size directly from source to target,
    /// bypassing the dump archive. Accepts `50GB`, `512MB`, `1024`, ...
    ///
    /// `pg_dump --jobs` parallelises per table, so one huge table always
    /// dumps on a single worker — and its bytes cross the network twice,
    /// into the archive and back out. Tables over this size are excluded
    /// from the archive and streamed directly in parallel `COPY` ranges
    /// instead. Off by default; unset means the previous behaviour.
    #[arg(long, value_name = "SIZE")]
    pub split_tables_larger_than: Option<String>,

    /// Concurrent COPY streams per split table (default 4). Measured
    /// throughput flattens around four — the target's write path saturates
    /// well before its CPU does.
    #[arg(long, default_value_t = 4, value_name = "N")]
    pub split_max_parts: usize,

    /// `pg_dump` compression spec passed to `--compress`. Examples:
    /// `gzip:6`, `zstd:3`, `lz4`, `none`. These `<algorithm>[:level]` forms
    /// (including `none`) need `pg_dump` 16+; PG ≤ 15 clients accept only a
    /// bare digit `0`-`9`, where `0` turns compression off. When unset,
    /// `pg_dump` picks its own default (typically `gzip`). Use `zstd:3` for
    /// the best CPU/ratio trade-off on modern hardware. Ignored when the
    /// directory format is not used (parallel dump implies directory).
    #[arg(long)]
    pub dump_compress: Option<String>,

    /// Disable the `--no-sync` perf flag passed to `pg_dump`. By default
    /// `pg_dump` is invoked with `--no-sync`, which skips the final
    /// `fsync(2)` over every output file. The dump archive is transient
    /// scratch state — losing it on a host crash just means re-running.
    /// Pass `--keep-sync` to restore the safer (and slower) default.
    /// (Note: `pg_restore` has no equivalent flag.)
    #[arg(long)]
    pub keep_sync: bool,

    /// Pass `--no-table-access-method` to `pg_dump` (PG 15+). Omits
    /// `USING <access_method>` clauses from CREATE TABLE statements.
    /// Useful when the target lacks the source's custom table AMs.
    #[arg(long)]
    pub no_table_access_method: bool,

    /// Skip the post-restore `ANALYZE` on the target database. By default
    /// pg_dbmigrator runs `ANALYZE` on all restored tables so the query
    /// planner has fresh statistics immediately. Pass this flag only if you
    /// run ANALYZE out-of-band or need minimum restore time.
    #[arg(long)]
    pub skip_analyze: bool,

    /// Skip the pre-dump `VACUUM ANALYZE` on the source database. By
    /// default pg_dbmigrator runs `VACUUM ANALYZE` on the source before
    /// `pg_dump` to ensure the dump reads clean heap pages and has fresh
    /// planner statistics for parallel-dump. Pass this flag when the
    /// source is under heavy write load and VACUUM I/O is unacceptable.
    #[arg(long)]
    pub skip_source_vacuum: bool,

    /// Post-migration row-count verification behaviour. `off` skips it;
    /// `warn` (default) logs mismatches and continues; `strict` makes a
    /// mismatch a hard error (non-zero exit). `--mode verify` is always strict.
    #[arg(long, value_enum, default_value_t = VerifyModeArg::Warn)]
    pub verify: VerifyModeArg,

    /// Run the source `VACUUM ANALYZE` and the target `ANALYZE` as
    /// `VERBOSE`. Does not change this program's log level — use `RUST_LOG`.
    #[arg(long)]
    pub verbose: bool,

    /// Emit machine-readable NDJSON progress events to stdout (one
    /// [`pg_dbmigrator::ProgressEvent`] per line). Human-readable tracing
    /// logs continue to go to stderr. Pair with
    /// `RUST_LOG=warn,pg_dbmigrator=warn` to silence stderr for clean piping.
    #[arg(long)]
    pub json: bool,

    /// Connect to source and target, print the PostgreSQL client major version
    /// that fits both (the newer of the two servers) on stdout, and exit
    /// without migrating. `install.sh` uses this to decide which
    /// `postgresql-client` package to install: the newer server dictates the
    /// floor for `pg_dump`, and staying at that version rather than the newest
    /// available avoids a newer `pg_restore` emitting settings the older
    /// target does not recognise.
    #[arg(long)]
    pub print_client_major: bool,
}

/// CLI-friendly mirror of [`MigrationMode`].
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ModeArg {
    Offline,
    Online,
    Verify,
}

impl From<ModeArg> for MigrationMode {
    fn from(value: ModeArg) -> Self {
        match value {
            ModeArg::Offline => MigrationMode::Offline,
            ModeArg::Online => MigrationMode::Online,
            ModeArg::Verify => MigrationMode::Verify,
        }
    }
}

/// CLI-friendly mirror of [`DumpScope`].
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum DumpScopeArg {
    All,
    SchemaOnly,
    DataOnly,
}

impl From<DumpScopeArg> for DumpScope {
    fn from(value: DumpScopeArg) -> Self {
        match value {
            DumpScopeArg::All => DumpScope::All,
            DumpScopeArg::SchemaOnly => DumpScope::SchemaOnly,
            DumpScopeArg::DataOnly => DumpScope::DataOnly,
        }
    }
}

/// CLI-friendly mirror of [`VerifyMode`].
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum VerifyModeArg {
    /// Skip verification entirely.
    Off,
    /// Run verification; warn on mismatch and continue (default).
    Warn,
    /// Run verification; a mismatch is a hard error (non-zero exit).
    Strict,
}

impl From<VerifyModeArg> for VerifyMode {
    fn from(value: VerifyModeArg) -> Self {
        match value {
            VerifyModeArg::Off => VerifyMode::Off,
            VerifyModeArg::Warn => VerifyMode::Warn,
            VerifyModeArg::Strict => VerifyMode::Strict,
        }
    }
}

impl Cli {
    /// Convert CLI args into the library [`MigrationConfig`].
    pub fn into_config(self) -> Result<MigrationConfig, pg_dbmigrator::MigrationError> {
        let source = EndpointConfig::parse(&self.source)?;
        let target = EndpointConfig::parse(&self.target)?;

        let apply = ReplicationApplyConfig {
            max_runtime_seconds: self.max_runtime_seconds,
            ..ReplicationApplyConfig::default()
        };

        let online = OnlineOptions {
            slot_name: self.slot_name,
            publication: self.publication,
            protocol_version: self.protocol_version,
            subscription_name: self.subscription_name,
            subscription_source_conn: self.subscription_source,
            drop_subscription_on_cutover: !self.keep_subscription,
            force_clean: self.force_clean,
            sync_sequences_on_cutover: !self.no_sequence_sync,
            auto_create_publication: !self.no_auto_create_publication,
            drop_slot_on_cutover: !self.keep_slot,
            apply,
            cutover: CutoverConfig {
                poll_interval: std::time::Duration::from_secs(self.cutover_poll_secs),
                fast_poll_interval: std::time::Duration::from_millis(self.cutover_fast_poll_ms),
                lag_threshold_bytes: self.lag_threshold_bytes,
            },
        };

        Ok(MigrationConfig {
            mode: self.mode.into(),
            source,
            target,
            dump_scope: self.dump_scope.into(),
            drop_target_first: self.drop_target_first,
            jobs: self.jobs,
            schemas: self.schemas,
            tables: self.tables,
            exclude_schemas: self.exclude_schemas,
            exclude_tables: self.exclude_tables,
            online,
            allow_restore_errors: self.allow_restore_errors,
            no_publications: !self.keep_publications,
            no_subscriptions: !self.keep_subscriptions,
            split_sections: self.split_sections && !self.no_split_sections,
            split_tables_larger_than: self
                .split_tables_larger_than
                .as_deref()
                .map(pg_dbmigrator::copy_split::parse_size)
                .transpose()?,
            split_max_parts: self.split_max_parts,
            resume: self.resume,
            resume_file: self.resume_file,
            dump_path: self.dump_path,
            verbose: self.verbose,
            dump_compress: self.dump_compress,
            no_sync: !self.keep_sync,
            no_comments: true,
            no_security_labels: true,
            no_table_access_method: self.no_table_access_method,
            skip_analyze: self.skip_analyze,
            skip_source_vacuum: self.skip_source_vacuum,
            verify: self.verify.into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    fn parse_args(args: &[&str]) -> Cli {
        Cli::parse_from(args)
    }

    #[test]
    fn minimal_offline_args_parse() {
        let cli = parse_args(&[
            "pg_dbmigrator",
            "--source",
            "postgresql://u@src/db",
            "--target",
            "postgresql://u@dst/db",
        ]);
        assert!(matches!(cli.mode, ModeArg::Offline));
        assert_eq!(cli.source, "postgresql://u@src/db");
        assert_eq!(cli.target, "postgresql://u@dst/db");
    }

    #[test]
    fn online_mode_parses() {
        let cli = parse_args(&[
            "pg_dbmigrator",
            "--mode",
            "online",
            "--source",
            "postgresql://u@src/db",
            "--target",
            "postgresql://u@dst/db",
        ]);
        assert!(matches!(cli.mode, ModeArg::Online));
    }

    #[test]
    fn into_config_offline_defaults() {
        let cli = parse_args(&[
            "pg_dbmigrator",
            "--source",
            "postgresql://u:p@src/db",
            "--target",
            "postgresql://u:p@dst/db",
        ]);
        let cfg = cli.into_config().unwrap();
        assert!(matches!(cfg.mode, MigrationMode::Offline));
        assert_eq!(cfg.source.host, "src");
        assert_eq!(cfg.target.host, "dst");
        assert!(cfg.no_publications);
        assert!(cfg.no_subscriptions);
        assert!(cfg.split_sections);
        assert!(cfg.no_sync);
        assert!(cfg.no_comments);
        assert!(cfg.no_security_labels);
        assert!(!cfg.no_table_access_method);
        assert!(!cfg.allow_restore_errors);
        assert!(!cfg.resume);
    }

    #[test]
    fn into_config_online_with_all_flags() {
        let cli = parse_args(&[
            "pg_dbmigrator",
            "--mode",
            "online",
            "--source",
            "postgresql://u:p@src/db",
            "--target",
            "postgresql://u:p@dst/db",
            "--slot-name",
            "my_slot",
            "--publication",
            "my_pub",
            "--subscription-name",
            "my_sub",
            "--keep-subscription",
            "--force-clean",
            "--no-sequence-sync",
            "--lag-threshold-bytes",
            "4096",
            "--cutover-poll-secs",
            "10",
            "--cutover-fast-poll-ms",
            "500",
            "--max-runtime-seconds",
            "3600",
        ]);
        let cfg = cli.into_config().unwrap();
        assert!(matches!(cfg.mode, MigrationMode::Online));
        assert_eq!(cfg.online.slot_name, "my_slot");
        assert_eq!(cfg.online.publication, "my_pub");
        assert_eq!(cfg.online.subscription_name, "my_sub");
        assert!(!cfg.online.drop_subscription_on_cutover);
        assert!(cfg.online.force_clean);
        assert!(!cfg.online.sync_sequences_on_cutover);
        assert_eq!(cfg.online.cutover.lag_threshold_bytes, 4096);
        assert_eq!(
            cfg.online.cutover.poll_interval,
            std::time::Duration::from_secs(10)
        );
        assert_eq!(
            cfg.online.cutover.fast_poll_interval,
            std::time::Duration::from_millis(500)
        );
        assert_eq!(cfg.online.apply.max_runtime_seconds, Some(3600));
    }

    #[test]
    fn into_config_exclude_flags() {
        let cli = parse_args(&[
            "pg_dbmigrator",
            "--source",
            "postgresql://u@src/db",
            "--target",
            "postgresql://u@dst/db",
            "--exclude-schema",
            "audit",
            "--exclude-schema",
            "temp",
            "--exclude-table",
            "public.big_log",
        ]);
        let cfg = cli.into_config().unwrap();
        assert_eq!(cfg.exclude_schemas, vec!["audit", "temp"]);
        assert_eq!(cfg.exclude_tables, vec!["public.big_log"]);
    }

    #[test]
    fn into_config_no_split_sections_overrides_default() {
        let cli = parse_args(&[
            "pg_dbmigrator",
            "--source",
            "postgresql://u@src/db",
            "--target",
            "postgresql://u@dst/db",
            "--no-split-sections",
        ]);
        let cfg = cli.into_config().unwrap();
        assert!(!cfg.split_sections);
    }

    #[test]
    fn into_config_keep_sync_disables_no_sync() {
        let cli = parse_args(&[
            "pg_dbmigrator",
            "--source",
            "postgresql://u@src/db",
            "--target",
            "postgresql://u@dst/db",
            "--keep-sync",
        ]);
        let cfg = cli.into_config().unwrap();
        assert!(!cfg.no_sync);
    }

    #[test]
    fn into_config_keep_publications_and_subscriptions() {
        let cli = parse_args(&[
            "pg_dbmigrator",
            "--source",
            "postgresql://u@src/db",
            "--target",
            "postgresql://u@dst/db",
            "--keep-publications",
            "--keep-subscriptions",
        ]);
        let cfg = cli.into_config().unwrap();
        assert!(!cfg.no_publications);
        assert!(!cfg.no_subscriptions);
    }

    #[test]
    fn into_config_dump_compress() {
        let cli = parse_args(&[
            "pg_dbmigrator",
            "--source",
            "postgresql://u@src/db",
            "--target",
            "postgresql://u@dst/db",
            "--dump-compress",
            "zstd:3",
        ]);
        let cfg = cli.into_config().unwrap();
        assert_eq!(cfg.dump_compress, Some("zstd:3".to_string()));
    }

    #[test]
    fn into_config_resume_and_dump_path() {
        let cli = parse_args(&[
            "pg_dbmigrator",
            "--source",
            "postgresql://u@src/db",
            "--target",
            "postgresql://u@dst/db",
            "--resume",
            "--dump-path",
            "/tmp/my_dump",
        ]);
        let cfg = cli.into_config().unwrap();
        assert!(cfg.resume);
        assert_eq!(cfg.dump_path, Some(PathBuf::from("/tmp/my_dump")));
    }

    #[test]
    fn into_config_allow_restore_errors() {
        let cli = parse_args(&[
            "pg_dbmigrator",
            "--source",
            "postgresql://u@src/db",
            "--target",
            "postgresql://u@dst/db",
            "--allow-restore-errors",
        ]);
        let cfg = cli.into_config().unwrap();
        assert!(cfg.allow_restore_errors);
    }

    #[test]
    fn into_config_dump_scope_schema_only() {
        let cli = parse_args(&[
            "pg_dbmigrator",
            "--source",
            "postgresql://u@src/db",
            "--target",
            "postgresql://u@dst/db",
            "--dump-scope",
            "schema-only",
        ]);
        let cfg = cli.into_config().unwrap();
        assert_eq!(cfg.dump_scope, DumpScope::SchemaOnly);
    }

    #[test]
    fn into_config_dump_scope_data_only() {
        let cli = parse_args(&[
            "pg_dbmigrator",
            "--source",
            "postgresql://u@src/db",
            "--target",
            "postgresql://u@dst/db",
            "--dump-scope",
            "data-only",
        ]);
        let cfg = cli.into_config().unwrap();
        assert_eq!(cfg.dump_scope, DumpScope::DataOnly);
    }

    #[test]
    fn into_config_no_table_access_method() {
        let cli = parse_args(&[
            "pg_dbmigrator",
            "--source",
            "postgresql://u@src/db",
            "--target",
            "postgresql://u@dst/db",
            "--no-table-access-method",
        ]);
        let cfg = cli.into_config().unwrap();
        assert!(cfg.no_table_access_method);
    }

    #[test]
    fn into_config_verbose() {
        let cli = parse_args(&[
            "pg_dbmigrator",
            "--source",
            "postgresql://u@src/db",
            "--target",
            "postgresql://u@dst/db",
            "--verbose",
        ]);
        let cfg = cli.into_config().unwrap();
        assert!(cfg.verbose);
    }

    #[test]
    fn mode_arg_into_migration_mode() {
        assert!(matches!(
            MigrationMode::from(ModeArg::Offline),
            MigrationMode::Offline
        ));
        assert!(matches!(
            MigrationMode::from(ModeArg::Online),
            MigrationMode::Online
        ));
    }

    #[test]
    fn dump_scope_arg_into_dump_scope() {
        assert_eq!(DumpScope::from(DumpScopeArg::All), DumpScope::All);
        assert_eq!(
            DumpScope::from(DumpScopeArg::SchemaOnly),
            DumpScope::SchemaOnly
        );
        assert_eq!(DumpScope::from(DumpScopeArg::DataOnly), DumpScope::DataOnly);
    }

    #[test]
    fn into_config_subscription_source_override() {
        let cli = parse_args(&[
            "pg_dbmigrator",
            "--mode",
            "online",
            "--source",
            "postgresql://u@src/db",
            "--target",
            "postgresql://u@dst/db",
            "--subscription-source",
            "postgresql://u@internal-src/db",
        ]);
        let cfg = cli.into_config().unwrap();
        assert_eq!(
            cfg.online.subscription_source_conn,
            Some("postgresql://u@internal-src/db".to_string())
        );
    }

    #[test]
    fn into_config_schemas_and_tables() {
        let cli = parse_args(&[
            "pg_dbmigrator",
            "--source",
            "postgresql://u@src/db",
            "--target",
            "postgresql://u@dst/db",
            "--schema",
            "public",
            "--schema",
            "app",
            "--table",
            "public.users",
        ]);
        let cfg = cli.into_config().unwrap();
        assert_eq!(cfg.schemas, vec!["public", "app"]);
        assert_eq!(cfg.tables, vec!["public.users"]);
    }

    #[test]
    fn into_config_drop_target_first() {
        let cli = parse_args(&[
            "pg_dbmigrator",
            "--source",
            "postgresql://u@src/db",
            "--target",
            "postgresql://u@dst/db",
            "--drop-target-first",
        ]);
        let cfg = cli.into_config().unwrap();
        assert!(cfg.drop_target_first);
    }

    #[test]
    fn into_config_invalid_source_url_returns_error() {
        let cli = parse_args(&[
            "pg_dbmigrator",
            "--source",
            "not-a-url",
            "--target",
            "postgresql://u@dst/db",
        ]);
        assert!(cli.into_config().is_err());
    }

    #[test]
    fn into_config_skip_analyze_flag() {
        let cli = parse_args(&[
            "pg_dbmigrator",
            "--source",
            "postgresql://u@src/db",
            "--target",
            "postgresql://u@dst/db",
            "--skip-analyze",
        ]);
        let cfg = cli.into_config().unwrap();
        assert!(cfg.skip_analyze);
    }

    #[test]
    fn into_config_skip_source_vacuum_flag() {
        let cli = parse_args(&[
            "pg_dbmigrator",
            "--source",
            "postgresql://u@src/db",
            "--target",
            "postgresql://u@dst/db",
            "--skip-source-vacuum",
        ]);
        let cfg = cli.into_config().unwrap();
        assert!(cfg.skip_source_vacuum);
    }

    #[test]
    fn into_config_defaults_analyze_and_vacuum_enabled() {
        let cli = parse_args(&[
            "pg_dbmigrator",
            "--source",
            "postgresql://u@src/db",
            "--target",
            "postgresql://u@dst/db",
        ]);
        let cfg = cli.into_config().unwrap();
        assert!(!cfg.skip_analyze);
        assert!(!cfg.skip_source_vacuum);
    }

    #[test]
    fn into_config_no_auto_create_publication() {
        let cli = parse_args(&[
            "pg_dbmigrator",
            "--mode",
            "online",
            "--source",
            "postgresql://u@src/db",
            "--target",
            "postgresql://u@dst/db",
            "--no-auto-create-publication",
        ]);
        let cfg = cli.into_config().unwrap();
        assert!(!cfg.online.auto_create_publication);
    }

    #[test]
    fn into_config_keep_slot() {
        let cli = parse_args(&[
            "pg_dbmigrator",
            "--mode",
            "online",
            "--source",
            "postgresql://u@src/db",
            "--target",
            "postgresql://u@dst/db",
            "--keep-slot",
        ]);
        let cfg = cli.into_config().unwrap();
        assert!(!cfg.online.drop_slot_on_cutover);
    }

    #[test]
    fn into_config_verify_mode_strict() {
        let cli = parse_args(&[
            "pg_dbmigrator",
            "--mode",
            "verify",
            "--source",
            "postgres://u:p@src/db",
            "--target",
            "postgres://u:p@dst/db",
            "--verify",
            "strict",
        ]);
        let cfg = cli.into_config().unwrap();
        assert_eq!(cfg.mode, pg_dbmigrator::MigrationMode::Verify);
        assert_eq!(cfg.verify, VerifyMode::Strict);
    }

    #[test]
    fn into_config_verify_off() {
        let cli = parse_args(&[
            "pg_dbmigrator",
            "--mode",
            "offline",
            "--source",
            "postgres://u:p@src/db",
            "--target",
            "postgres://u:p@dst/db",
            "--verify",
            "off",
        ]);
        let cfg = cli.into_config().unwrap();
        assert_eq!(cfg.verify, VerifyMode::Off);
    }

    #[test]
    fn into_config_verify_defaults_to_warn() {
        let cli = parse_args(&[
            "pg_dbmigrator",
            "--source",
            "postgres://u:p@src/db",
            "--target",
            "postgres://u:p@dst/db",
        ]);
        let cfg = cli.into_config().unwrap();
        assert_eq!(cfg.verify, VerifyMode::Warn);
    }

    #[test]
    fn into_config_defaults_auto_create_publication_and_drop_slot() {
        let cli = parse_args(&[
            "pg_dbmigrator",
            "--source",
            "postgresql://u@src/db",
            "--target",
            "postgresql://u@dst/db",
        ]);
        let cfg = cli.into_config().unwrap();
        assert!(cfg.online.auto_create_publication);
        assert!(cfg.online.drop_slot_on_cutover);
    }
}
