//! Configuration types used to drive a migration.

use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use url::Url;

use crate::error::{MigrationError, Result};

/// Top-level migration configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationConfig {
    /// Selects the migration strategy (see [`MigrationMode`]).
    pub mode: MigrationMode,
    /// Source PostgreSQL endpoint (the database to migrate _from_).
    pub source: EndpointConfig,
    /// Target PostgreSQL endpoint (the database to migrate _to_).
    pub target: EndpointConfig,
    /// What to dump (schema only, data only, or both).
    pub dump_scope: DumpScope,
    /// If `true`, drop and recreate the target schema before restore.
    pub drop_target_first: bool,
    /// Number of parallel jobs to pass to `pg_dump --jobs` / `pg_restore --jobs`.
    pub jobs: usize,
    /// Optional list of schemas to migrate. When empty, all schemas are migrated.
    pub schemas: Vec<String>,
    /// Optional list of tables (`schema.table`) to migrate.
    pub tables: Vec<String>,
    /// Optional list of schemas to **exclude** from the migration. Maps
    /// to `pg_dump --exclude-schema=<name>` (repeatable). Useful when
    /// the source has tenant schemas, audit schemas, or vendor-managed
    /// extension schemas that should not be replicated. Empty by
    /// default.
    #[serde(default)]
    pub exclude_schemas: Vec<String>,
    /// Optional list of tables (`schema.table`) to **exclude** from the
    /// migration. Maps to `pg_dump --exclude-table=<schema.table>`
    /// (repeatable). Empty by default.
    #[serde(default)]
    pub exclude_tables: Vec<String>,
    /// Online-only options — ignored in [`MigrationMode::Offline`].
    pub online: OnlineOptions,
    /// If `true`, treat `pg_restore` exit-1 (the conventional "completed
    /// with errors" signal) as a non-fatal warning rather than aborting.
    /// Required for cross-server migrations where the source has installed
    /// extensions whose internal state can't be re-created on the target
    /// (Azure-reserved extensions, extensions whose meta-tables only
    /// privileged roles can write to, etc.). Default `false`.
    #[serde(default)]
    pub allow_restore_errors: bool,
    /// Pass `--no-publications` to `pg_dump`. Default `true`. Source-side
    /// publications (e.g. our own `pg_dbmigrator_pub`) are migration scaffolding;
    /// recreating them on the target produces noise such as
    /// `wal_level is insufficient to publish logical changes` warnings.
    #[serde(default = "default_true")]
    pub no_publications: bool,
    /// Pass `--no-subscriptions` to `pg_dump`. Default `true`. A new target
    /// should not inherit subscription definitions that point at the previous
    /// upstream.
    #[serde(default = "default_true")]
    pub no_subscriptions: bool,
    /// If `true`, restore in three phases — `pre-data`, `data`, then
    /// `post-data` — instead of one all-in-one `pg_restore` call. Splitting
    /// lets the bulk `COPY` (data) phase run without index maintenance and
    /// then rebuilds every index in parallel against fully-loaded tables.
    /// On schemas with many secondary indexes this is typically 30–60 %
    /// faster than the default. Requires a directory- or custom-format
    /// dump (i.e. not [`crate::dump::DumpFormat::Plain`]). Default `true`.
    #[serde(default = "default_true")]
    pub split_sections: bool,
    /// Move tables at or above this many bytes with a direct parallel
    /// source → target `COPY` instead of routing them through the dump
    /// archive. `None` (the default) disables the feature entirely, and the
    /// dump/restore path behaves exactly as it did before.
    ///
    /// `pg_dump --jobs` parallelises across archive entries, and a table is
    /// one entry, so a single huge table is always dumped by one worker —
    /// and its bytes cross the network twice, once into the migrator's
    /// archive and once back out to the target. Tables selected here are
    /// excluded from the archive (`--exclude-table-data`) and streamed
    /// directly instead. See [`crate::copy_split`].
    #[serde(default)]
    pub split_tables_larger_than: Option<u64>,
    /// Concurrent `COPY` streams per split table. Default 4: measured
    /// throughput flattened there (four streams landed within 1 % of
    /// eight) because the target's write path saturates long before its
    /// CPU does, so extra streams mostly buy contention.
    #[serde(default = "default_split_max_parts")]
    pub split_max_parts: usize,
    /// Optional `--compress=<spec>` value forwarded to `pg_dump`. PG 16+
    /// accepts `gzip:N`, `lz4:N`, `zstd:N`, or `none`; older versions
    /// accept `0..=9`. Trades a small amount of source-side CPU for a 3–10×
    /// reduction in archive size — a clear win whenever the source ↔
    /// migrator hop crosses a region or VPN boundary. Library callers get
    /// `lz4:1` (negligible CPU overhead, 3–5× size reduction on typical
    /// data) from both [`Default`] and serde; the CLI does not apply that
    /// default — it leaves this `None` unless `--dump-compress` is passed.
    /// `None` omits `--compress` altogether, letting `pg_dump` fall back to
    /// its format's own default. That is also the portable way to turn
    /// compression off: the `none` spec needs `pg_dump` 16+, and PG ≤ 15
    /// accepts only a bare digit (`0`).
    #[serde(default = "default_compress")]
    pub dump_compress: Option<String>,
    /// Pass `--no-sync` to `pg_dump`. Default `true`. The dump archive
    /// is a transient artefact — fsyncing every output file is pure I/O
    /// overhead. Disable only if you intend to reuse the dump archive
    /// long after the migrator process has exited *and* you cannot
    /// afford to lose its tail to a crash. (No corresponding flag
    /// exists for `pg_restore`.)
    #[serde(default = "default_true")]
    pub no_sync: bool,
    /// Pass `--no-comments` to `pg_dump`. Default `true`. COMMENT ON
    /// statements are rarely needed on the target and add both dump
    /// size and restore time — skipping them is a free performance win
    /// for the common migration case. Set to `false` only when the
    /// target must carry over all user-defined comments from the source.
    #[serde(default = "default_true")]
    pub no_comments: bool,
    /// Pass `--no-security-labels` to `pg_dump`. Default `true`.
    /// Security labels (SE-Linux row-level security labels) are almost
    /// never relevant on the migration target and can add overhead.
    /// Set to `false` only when the target uses SE-Linux label-based
    /// access control identical to the source's.
    #[serde(default = "default_true")]
    pub no_security_labels: bool,
    /// Pass `--no-table-access-method` to `pg_dump`. Default `false`.
    /// PG 15+ supports this flag — it omits `USING <access_method>`
    /// clauses from CREATE TABLE statements. Useful when the target
    /// does not have the same access method extensions installed.
    /// Leave `false` unless you know the target lacks the source's AMs.
    #[serde(default)]
    pub no_table_access_method: bool,
    /// Skip `ANALYZE` on the target after restore completes. Default
    /// `false` (i.e. ANALYZE runs). After a bulk `pg_restore` the
    /// target's `pg_statistic` is empty — the query planner will produce
    /// terrible plans until `ANALYZE` runs. Set to `true` only if you
    /// run `ANALYZE` out-of-band or if restore time is more critical
    /// than first-query latency.
    #[serde(default)]
    pub skip_analyze: bool,
    /// Skip `VACUUM ANALYZE` on the source before dump. Default `false`
    /// (i.e. VACUUM ANALYZE runs). Running VACUUM ANALYZE on the source
    /// before `pg_dump` ensures the dump reads from clean, unfragmented
    /// heap pages and has fresh statistics for parallel-dump planning.
    /// Set to `true` when the source is under heavy write load and you
    /// cannot afford the I/O cost of a full VACUUM, or when you have
    /// just recently run maintenance manually.
    #[serde(default)]
    pub skip_source_vacuum: bool,
    /// Controls the automatic post-restore row-count verification step:
    /// `Off` skips it, `Warn` (default) logs mismatches and continues, and
    /// `Strict` treats a mismatch as a hard error (non-zero exit). Ignored
    /// when `mode == Verify`, which is always strict.
    #[serde(default)]
    pub verify: VerifyMode,
    /// If `true`, attempt to resume a previous run by reading the
    /// resume token at [`Self::resume_file`] (default
    /// `<dump_path>.resume.json`). Stages already marked complete in the
    /// token are skipped. Requires a stable [`Self::dump_path`] override
    /// so the orchestrator knows where the on-disk dump archive lives;
    /// validation rejects `resume=true` without an explicit dump path.
    /// Default `false`.
    #[serde(default)]
    pub resume: bool,
    /// Override for the resume token file path. When `None`, the
    /// orchestrator uses
    /// [`crate::resume::default_resume_path`]`(dump_path)`.
    #[serde(default)]
    pub resume_file: Option<PathBuf>,
    /// Pinned `pg_dump` archive path. Required when [`Self::resume`] is
    /// `true` so subsequent runs target the same archive. When `None`,
    /// the orchestrator generates a per-pid path under
    /// `std::env::temp_dir()`.
    #[serde(default)]
    pub dump_path: Option<PathBuf>,
    /// Verbose logging.
    pub verbose: bool,
}

fn default_true() -> bool {
    true
}

fn default_compress() -> Option<String> {
    Some("lz4:1".into())
}

/// Concurrent `COPY` streams per split table. See
/// [`MigrationConfig::split_max_parts`] for why the number is this low.
fn default_split_max_parts() -> usize {
    4
}

/// Pick a sensible default for `pg_dump --jobs` / `pg_restore --jobs`.
///
/// We use the host's logical CPU count (typically a good proxy for the
/// number of independent I/O streams the dump archive can absorb) but
/// clamp to `[1, 8]`. Above 8 the marginal gain falls off fast — the
/// shared catalog locks on the source and the contended buffer-pool
/// pages on the target start to matter. On hosts that can't report
/// parallelism, we fall back to 4 (the previous static default).
pub fn default_jobs() -> usize {
    std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(4)
        .clamp(1, 8)
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            mode: MigrationMode::Offline,
            source: EndpointConfig::default(),
            target: EndpointConfig::default(),
            dump_scope: DumpScope::All,
            drop_target_first: false,
            jobs: default_jobs(),
            schemas: Vec::new(),
            tables: Vec::new(),
            exclude_schemas: Vec::new(),
            exclude_tables: Vec::new(),
            online: OnlineOptions::default(),
            allow_restore_errors: false,
            no_publications: true,
            no_subscriptions: true,
            split_sections: true,
            split_tables_larger_than: None,
            split_max_parts: default_split_max_parts(),
            dump_compress: default_compress(),
            no_sync: true,
            no_comments: true,
            no_security_labels: true,
            no_table_access_method: false,
            skip_analyze: false,
            skip_source_vacuum: false,
            verify: VerifyMode::default(),
            resume: false,
            resume_file: None,
            dump_path: None,
            verbose: false,
        }
    }
}

impl MigrationConfig {
    /// Validate cross-field invariants. Called automatically by [`Migrator::run`].
    ///
    /// [`Migrator::run`]: crate::Migrator::run
    pub fn validate(&self) -> Result<()> {
        if self.source.connection_string.is_empty() {
            return Err(MigrationError::config("source connection string is empty"));
        }
        if self.target.connection_string.is_empty() {
            return Err(MigrationError::config("target connection string is empty"));
        }
        if self.jobs == 0 {
            return Err(MigrationError::config("jobs must be >= 1"));
        }
        if self.resume && self.dump_path.is_none() {
            return Err(MigrationError::config(
                "--resume requires --dump-path so subsequent runs target the same archive",
            ));
        }
        if self.mode == MigrationMode::Online {
            self.online.validate()?;
        }
        self.validate_split_copy()?;
        Ok(())
    }

    /// Reject the split-copy combinations that cannot be made correct.
    ///
    /// Each of these is a refusal rather than a silent degradation because
    /// the failure mode is data loss or data destruction, not slowness.
    fn validate_split_copy(&self) -> Result<()> {
        let Some(threshold) = self.split_tables_larger_than else {
            return Ok(());
        };
        if threshold == 0 {
            return Err(MigrationError::config(
                "--split-tables-larger-than must be > 0; a zero threshold would route every \
                 table in the database through the split copy",
            ));
        }
        if self.split_max_parts == 0 {
            return Err(MigrationError::config("--split-max-parts must be >= 1"));
        }
        if self.dump_scope == DumpScope::SchemaOnly {
            return Err(MigrationError::config(
                "--split-tables-larger-than moves table data and cannot be combined with a \
                 schema-only dump",
            ));
        }
        // The planner queries the source catalog directly and does not
        // reimplement pg_dump's pattern matching, so the two would disagree
        // about which tables are in scope — excluding a table's data from
        // the archive while still copying it directly, or the reverse.
        if !self.schemas.is_empty()
            || !self.tables.is_empty()
            || !self.exclude_schemas.is_empty()
            || !self.exclude_tables.is_empty()
        {
            return Err(MigrationError::config(
                "--split-tables-larger-than cannot yet be combined with --schema / --table / \
                 --exclude-schema / --exclude-table: the split planner does not implement \
                 pg_dump's pattern matching, so the two would disagree about which tables are \
                 in scope",
            ));
        }
        // An exported snapshot dies with the process that exported it, so a
        // resumed run cannot read the split tables at the same instant as
        // the archive it is resuming into. Re-deriving the plan is worse
        // still: a table that has since dropped below the threshold had its
        // data excluded from the archive and would never be copied at all.
        if self.resume {
            return Err(MigrationError::config(
                "--split-tables-larger-than cannot be combined with --resume: the exported \
                 snapshot the direct copy reads from does not survive a restart, so a resumed \
                 run cannot stay consistent with the archive it is resuming into",
            ));
        }
        // The all-in-one restore has already built indexes, foreign keys and
        // triggers by the time the copy would run, so the copy would fire
        // triggers pg_restore would not and TRUNCATE would fail outright on
        // an FK-referenced table.
        if !self.split_sections {
            return Err(MigrationError::config(
                "--split-tables-larger-than requires split-section restore; remove \
                 --no-split-sections. The direct copy has to land between the data and \
                 post-data sections, before indexes and foreign keys exist",
            ));
        }
        Ok(())
    }
}

/// Migration strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationMode {
    /// One-shot `pg_dump` followed by `pg_restore`. Source must be quiesced
    /// (or the lost-write window accepted) for the duration of the migration.
    Offline,
    /// Snapshot-based `pg_dump` + `pg_restore` followed by ongoing logical
    /// replication apply through `pg_walstream`.
    Online,
    /// Compare per-table row counts between source and target only. No dump,
    /// restore, or replication. Exits non-zero on any mismatch.
    Verify,
}

/// Controls the automatic post-migration row-count verification step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum VerifyMode {
    /// Skip verification entirely.
    Off,
    /// Run verification; log a warning on any mismatch but continue. Default.
    #[default]
    Warn,
    /// Run verification; a mismatch is a hard error (non-zero exit).
    Strict,
}

/// Choose what kind of dump to produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DumpScope {
    /// Both schema and data.
    All,
    /// Schema only (`--schema-only`).
    SchemaOnly,
    /// Data only (`--data-only`).
    DataOnly,
}

impl DumpScope {
    /// Returns the matching `pg_dump` command-line flag, if any.
    pub fn pg_dump_flag(self) -> Option<&'static str> {
        match self {
            Self::All => None,
            Self::SchemaOnly => Some("--schema-only"),
            Self::DataOnly => Some("--data-only"),
        }
    }
}

/// A single PostgreSQL endpoint description.
///
/// We store the original libpq URI verbatim in `connection_string` and parse
/// it once into individual fields so that callers do not need to do that
/// themselves.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EndpointConfig {
    /// Original libpq URI (e.g. `postgresql://user:pw@host:5432/db`).
    pub connection_string: String,
    /// Hostname, parsed from `connection_string`.
    pub host: String,
    /// TCP port, parsed from `connection_string` (defaults to 5432).
    pub port: u16,
    /// Database name, parsed from `connection_string`.
    pub database: String,
    /// Username, parsed from `connection_string` (may be empty).
    pub user: String,
    /// Password, parsed from `connection_string` (may be empty).
    pub password: String,
}

impl EndpointConfig {
    /// Parse a libpq URI into an [`EndpointConfig`].
    ///
    /// Accepts the `postgresql://` and `postgres://` schemes. Query parameters
    /// (e.g. `?sslmode=require`) are preserved on `connection_string` but not
    /// extracted into individual fields.
    pub fn parse(conn: &str) -> Result<Self> {
        let url = Url::parse(conn)
            .map_err(|e| MigrationError::InvalidConnectionString(format!("{conn}: {e}")))?;

        if !matches!(url.scheme(), "postgres" | "postgresql") {
            return Err(MigrationError::InvalidConnectionString(format!(
                "unsupported scheme `{}`",
                url.scheme()
            )));
        }

        let host = url
            .host_str()
            .ok_or_else(|| {
                MigrationError::InvalidConnectionString(format!("{conn}: missing host"))
            })?
            .to_string();
        let port = url.port().unwrap_or(5432);
        let database = url
            .path()
            .trim_start_matches('/')
            .split('?')
            .next()
            .unwrap_or("")
            .to_string();
        let user = url.username().to_string();
        let password = url.password().unwrap_or("").to_string();

        Ok(Self {
            connection_string: conn.to_string(),
            host,
            port,
            database,
            user,
            password,
        })
    }

    /// Returns a redacted form suitable for logging (password is masked).
    pub fn redacted(&self) -> String {
        if self.password.is_empty() {
            self.connection_string.clone()
        } else {
            self.connection_string
                .replacen(&format!(":{}@", self.password), ":****@", 1)
        }
    }
}

/// Online-mode-specific knobs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnlineOptions {
    /// Replication slot name on the source. Will be created with
    /// `EXPORT_SNAPSHOT` if it does not already exist.
    pub slot_name: String,
    /// Publication name on the source. The library does not create the
    /// publication automatically; this must be done by the operator.
    pub publication: String,
    /// pgoutput protocol version (2 enables streaming).
    pub protocol_version: u32,
    /// Subscription name created on the target. The library issues
    /// `CREATE SUBSCRIPTION ... WITH (create_slot=false, slot_name='<existing>',
    /// enabled=true, copy_data=false)` so PostgreSQL's built-in apply worker
    /// streams from the slot prepared during `PrepareSnapshot`.
    #[serde(default = "default_subscription_name")]
    pub subscription_name: String,
    /// Override for the source connection string written into
    /// `CREATE SUBSCRIPTION ... CONNECTION '<…>'`.
    ///
    /// `None` means the same URI used by the migrator (`source.connection_string`)
    /// is reused. Set this when the migrator and the target's apply worker
    /// see the source from different network locations (e.g. operator runs
    /// from outside a Docker network while the target connects via the
    /// in-network service name). Always validated by the target — the apply
    /// worker, not the migrator, dials this URI.
    #[serde(default)]
    pub subscription_source_conn: Option<String>,
    /// If `true`, drop the subscription after a successful cutover. If
    /// `false`, the subscription is left disabled but in place so the
    /// operator can inspect it. Default `true`.
    #[serde(default = "default_true")]
    pub drop_subscription_on_cutover: bool,
    /// If `true`, run a best-effort cleanup before creating the slot &
    /// subscription: any leftover subscription with the same name on the
    /// target is disabled / detached / dropped, and any leftover replication
    /// slot with the same name on the source is dropped. Use this when a
    /// previous run died after `CREATE SUBSCRIPTION` and the next run would
    /// otherwise fail with "subscription already exists" / "slot already
    /// exists". Default `false` so operators opt in explicitly. CLI:
    /// `--force-clean`.
    #[serde(default)]
    pub force_clean: bool,
    /// If `true` (default), sync every user sequence from source to
    /// target right after the operator triggers cutover but before the
    /// migration returns. PostgreSQL logical replication does **not**
    /// replay sequence advances, so without this step the target's
    /// sequences stay at their dump-time values and the first
    /// post-cutover `INSERT … DEFAULT nextval(...)` will produce a
    /// duplicate-key violation. Disable only if you have your own
    /// out-of-band sequence sync (e.g. application-level UUIDs).
    #[serde(default = "default_true")]
    pub sync_sequences_on_cutover: bool,
    /// If `true` (default), automatically create the publication on the source if it does not already exist. The publication will be created as `FOR ALL TABLES` (or scoped by  `MigrationConfig::tables` / `MigrationConfig::schemas` if set). Publications auto-created by the migrator are tracked and dropped  after cutover.
    #[serde(default = "default_true")]
    pub auto_create_publication: bool,
    /// If `true` (default), drop the replication slot on the source after a successful cutover. The slot is no longer needed once the target has caught up and the subscription is torn down.
    #[serde(default = "default_true")]
    pub drop_slot_on_cutover: bool,
    /// Configuration for the WAL apply worker.
    pub apply: ReplicationApplyConfig,
    /// Cutover knobs — when to declare the target "caught up" and how the
    /// operator triggers cutover.
    pub cutover: CutoverConfig,
}

fn default_subscription_name() -> String {
    "pg_dbmigrator_sub".to_string()
}

impl Default for OnlineOptions {
    fn default() -> Self {
        Self {
            slot_name: "pg_dbmigrator_slot".to_string(),
            publication: "pg_dbmigrator_pub".to_string(),
            protocol_version: 2,
            subscription_name: default_subscription_name(),
            subscription_source_conn: None,
            drop_subscription_on_cutover: true,
            force_clean: false,
            sync_sequences_on_cutover: true,
            auto_create_publication: true,
            drop_slot_on_cutover: true,
            apply: ReplicationApplyConfig::default(),
            cutover: CutoverConfig::default(),
        }
    }
}

impl OnlineOptions {
    /// Validate the online-specific fields.
    pub fn validate(&self) -> Result<()> {
        if self.slot_name.is_empty() {
            return Err(MigrationError::config("slot_name must not be empty"));
        }
        if self.publication.is_empty() {
            return Err(MigrationError::config("publication must not be empty"));
        }
        if self.protocol_version == 0 || self.protocol_version > 4 {
            return Err(MigrationError::config("protocol_version must be in 1..=4"));
        }
        if self.subscription_name.is_empty() {
            return Err(MigrationError::config(
                "subscription_name must not be empty",
            ));
        }
        self.cutover.validate()?;
        Ok(())
    }
}

/// Tunables for the streaming apply loop.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationApplyConfig {
    /// How often to send standby status updates to the source.
    #[serde(with = "humantime_serde_workaround")]
    pub feedback_interval: Duration,
    /// Connection timeout for both source and target connections.
    #[serde(with = "humantime_serde_workaround")]
    pub connection_timeout: Duration,
    /// Health-check / keep-alive interval for the source connection.
    #[serde(with = "humantime_serde_workaround")]
    pub health_check_interval: Duration,
    /// Stop the apply loop after this many seconds of catch-up. `None` means
    /// run until cancelled.
    pub max_runtime_seconds: Option<u64>,
}

impl Default for ReplicationApplyConfig {
    fn default() -> Self {
        Self {
            feedback_interval: Duration::from_secs(10),
            connection_timeout: Duration::from_secs(30),
            health_check_interval: Duration::from_secs(60),
            max_runtime_seconds: None,
        }
    }
}

/// Cutover policy.
///
/// Online migrations stream WAL until the operator decides to cut over.
/// Once the lag (in bytes between `last_applied_lsn` and the source's
/// current WAL flush position) first drops at or below
/// `lag_threshold_bytes`, the orchestrator emits an advisory
/// [`crate::progress::MigrationStage::CaughtUp`] event so the operator
/// knows it is the cheapest moment to switch. Cutover itself only happens
/// when the operator calls
/// [`crate::cutover::CutoverHandle::request`] (the CLI wires this to
/// SIGINT / Ctrl+C).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CutoverConfig {
    /// How often to query the source's current WAL LSN while the lag is
    /// still high. The default is intentionally coarse so we don't spam
    /// the source with cheap-but-not-free `pg_current_wal_flush_lsn()`
    /// queries during a multi-hour catch-up.
    #[serde(with = "humantime_serde_workaround")]
    pub poll_interval: Duration,
    /// Tighter cadence used once the lag drops at or below
    /// `lag_threshold_bytes` (i.e. the operator might cut over at any
    /// moment). Defaults to 1 s — the goal is that the operator's
    /// SIGINT lands on the apply loop within a second instead of
    /// being capped by `poll_interval`.
    #[serde(with = "humantime_serde_workaround", default = "default_fast_poll")]
    pub fast_poll_interval: Duration,
    /// Advisory lag threshold (in WAL bytes). When the lag first drops at
    /// or below this value the orchestrator emits a one-shot `CaughtUp`
    /// ("ready for cutover") event. The threshold never triggers cutover
    /// on its own — that is always operator-driven.
    pub lag_threshold_bytes: u64,
}

fn default_fast_poll() -> Duration {
    Duration::from_secs(1)
}

impl Default for CutoverConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(5),
            fast_poll_interval: default_fast_poll(),
            // 8 KiB — one WAL page; "ready" threshold for short-lived idle gaps.
            lag_threshold_bytes: 8 * 1024,
        }
    }
}

impl CutoverConfig {
    /// Validate cutover knobs.
    pub fn validate(&self) -> Result<()> {
        if self.poll_interval.is_zero() {
            return Err(MigrationError::config("cutover.poll_interval must be > 0"));
        }
        if self.fast_poll_interval.is_zero() {
            return Err(MigrationError::config(
                "cutover.fast_poll_interval must be > 0",
            ));
        }
        Ok(())
    }
}

/// Tiny `serde` adapter for `std::time::Duration` ↔ seconds. Avoids pulling in
/// the `humantime` crate for what is otherwise a one-line conversion.
mod humantime_serde_workaround {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        d.as_secs().serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        let secs = u64::deserialize(d)?;
        Ok(Duration::from_secs(secs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_basic_uri() {
        let ep =
            EndpointConfig::parse("postgresql://alice:s3cret@db.example:5433/app").expect("parse");
        assert_eq!(ep.host, "db.example");
        assert_eq!(ep.port, 5433);
        assert_eq!(ep.database, "app");
        assert_eq!(ep.user, "alice");
        assert_eq!(ep.password, "s3cret");
    }

    #[test]
    fn parses_default_port() {
        let ep = EndpointConfig::parse("postgres://u@h/db").unwrap();
        assert_eq!(ep.port, 5432);
        assert_eq!(ep.user, "u");
        assert!(ep.password.is_empty());
    }

    #[test]
    fn rejects_bad_scheme() {
        let err = EndpointConfig::parse("mysql://u@h/db").unwrap_err();
        assert!(matches!(err, MigrationError::InvalidConnectionString(_)));
    }

    #[test]
    fn rejects_missing_host() {
        let err = EndpointConfig::parse("postgresql:///db").unwrap_err();
        assert!(matches!(err, MigrationError::InvalidConnectionString(_)));
    }

    #[test]
    fn redacted_masks_password() {
        let ep = EndpointConfig::parse("postgresql://u:topsecret@h/db").unwrap();
        let redacted = ep.redacted();
        assert!(!redacted.contains("topsecret"));
        assert!(redacted.contains(":****@"));
    }

    #[test]
    fn redacted_passthrough_when_no_password() {
        let ep = EndpointConfig::parse("postgresql://u@h/db").unwrap();
        assert_eq!(ep.redacted(), "postgresql://u@h/db");
    }

    #[test]
    fn validate_rejects_zero_jobs() {
        let cfg = MigrationConfig {
            source: EndpointConfig::parse("postgres://u@s/db").unwrap(),
            target: EndpointConfig::parse("postgres://u@t/db").unwrap(),
            jobs: 0,
            ..MigrationConfig::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, MigrationError::Config(_)));
    }

    #[test]
    fn online_options_validate() {
        let mut opts = OnlineOptions::default();
        assert!(opts.validate().is_ok());

        opts.slot_name.clear();
        assert!(opts.validate().is_err());

        opts.slot_name = "s".into();
        opts.publication.clear();
        assert!(opts.validate().is_err());

        opts.publication = "p".into();
        opts.protocol_version = 99;
        assert!(opts.validate().is_err());
    }

    #[test]
    fn dump_scope_flag_mapping() {
        assert_eq!(DumpScope::All.pg_dump_flag(), None);
        assert_eq!(DumpScope::SchemaOnly.pg_dump_flag(), Some("--schema-only"));
        assert_eq!(DumpScope::DataOnly.pg_dump_flag(), Some("--data-only"));
    }

    #[test]
    fn cutover_config_default_is_valid() {
        let c = CutoverConfig::default();
        assert!(c.validate().is_ok());
        assert_eq!(c.lag_threshold_bytes, 8 * 1024);
    }

    #[test]
    fn cutover_config_rejects_zero_poll_interval() {
        let c = CutoverConfig {
            poll_interval: Duration::from_secs(0),
            ..CutoverConfig::default()
        };
        let err = c.validate().unwrap_err();
        assert!(matches!(err, MigrationError::Config(_)));
    }

    #[test]
    fn online_options_validate_propagates_cutover_error() {
        let opts = OnlineOptions {
            cutover: CutoverConfig {
                poll_interval: Duration::from_secs(0),
                ..CutoverConfig::default()
            },
            ..OnlineOptions::default()
        };
        assert!(opts.validate().is_err());
    }

    #[test]
    fn online_options_default_subscription_name() {
        let opts = OnlineOptions::default();
        assert_eq!(opts.subscription_name, "pg_dbmigrator_sub");
        assert!(opts.drop_subscription_on_cutover);
    }

    #[test]
    fn online_options_reject_empty_subscription_name() {
        let opts = OnlineOptions {
            subscription_name: String::new(),
            ..OnlineOptions::default()
        };
        assert!(opts.validate().is_err());
    }

    #[test]
    fn online_options_default_syncs_sequences_on_cutover() {
        // Online migrations MUST default to syncing sequences. PG
        // logical replication does not replay nextval(), so leaving
        // this off silently breaks every post-cutover INSERT.
        let opts = OnlineOptions::default();
        assert!(opts.sync_sequences_on_cutover);
    }

    #[test]
    fn migration_config_default_has_empty_exclude_lists() {
        let cfg = MigrationConfig::default();
        assert!(cfg.exclude_schemas.is_empty());
        assert!(cfg.exclude_tables.is_empty());
    }

    #[test]
    fn default_jobs_returns_value_in_valid_range() {
        let j = default_jobs();
        assert!((1..=8).contains(&j));
    }

    #[test]
    fn migration_config_default_performance_fields() {
        let cfg = MigrationConfig::default();
        assert!(cfg.split_sections);
        assert_eq!(cfg.dump_compress, Some("lz4:1".to_string()));
        assert!(cfg.no_sync);
        assert!(cfg.no_comments);
        assert!(cfg.no_security_labels);
        assert!(!cfg.no_table_access_method);
        assert!(cfg.no_publications);
        assert!(cfg.no_subscriptions);
    }

    #[test]
    fn validate_rejects_empty_source() {
        let cfg = MigrationConfig {
            source: EndpointConfig::default(),
            target: EndpointConfig::parse("postgres://u@t/db").unwrap(),
            ..MigrationConfig::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(err
            .to_string()
            .contains("source connection string is empty"));
    }

    #[test]
    fn validate_rejects_empty_target() {
        let cfg = MigrationConfig {
            source: EndpointConfig::parse("postgres://u@s/db").unwrap(),
            target: EndpointConfig::default(),
            ..MigrationConfig::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(err
            .to_string()
            .contains("target connection string is empty"));
    }

    #[test]
    fn validate_rejects_resume_without_dump_path() {
        let cfg = MigrationConfig {
            source: EndpointConfig::parse("postgres://u@s/db").unwrap(),
            target: EndpointConfig::parse("postgres://u@t/db").unwrap(),
            resume: true,
            dump_path: None,
            ..MigrationConfig::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("--resume requires --dump-path"));
    }

    #[test]
    fn validate_accepts_valid_offline_config() {
        let cfg = MigrationConfig {
            source: EndpointConfig::parse("postgres://u@s/db").unwrap(),
            target: EndpointConfig::parse("postgres://u@t/db").unwrap(),
            ..MigrationConfig::default()
        };
        cfg.validate().unwrap();
    }

    #[test]
    fn validate_online_propagates_online_options_error() {
        let cfg = MigrationConfig {
            mode: MigrationMode::Online,
            source: EndpointConfig::parse("postgres://u@s/db").unwrap(),
            target: EndpointConfig::parse("postgres://u@t/db").unwrap(),
            online: OnlineOptions {
                slot_name: String::new(),
                ..OnlineOptions::default()
            },
            ..MigrationConfig::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("slot_name"));
    }

    #[test]
    fn cutover_config_rejects_zero_fast_poll_interval() {
        let c = CutoverConfig {
            fast_poll_interval: Duration::from_secs(0),
            ..CutoverConfig::default()
        };
        let err = c.validate().unwrap_err();
        assert!(err.to_string().contains("fast_poll_interval"));
    }

    #[test]
    fn migration_config_serde_roundtrip() {
        let cfg = MigrationConfig {
            source: EndpointConfig::parse("postgres://u@s/db").unwrap(),
            target: EndpointConfig::parse("postgres://u@t/db").unwrap(),
            ..MigrationConfig::default()
        };
        let json = serde_json::to_string(&cfg).unwrap();
        let cfg2: MigrationConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(cfg2.jobs, cfg.jobs);
        assert_eq!(cfg2.split_sections, cfg.split_sections);
        assert_eq!(cfg2.dump_compress, cfg.dump_compress);
        assert_eq!(cfg2.no_sync, cfg.no_sync);
        assert_eq!(cfg2.mode as u8, cfg.mode as u8);
    }

    #[test]
    fn endpoint_config_parses_query_params() {
        let ep =
            EndpointConfig::parse("postgresql://u:p@h:5432/db?sslmode=require&application_name=x")
                .unwrap();
        assert_eq!(ep.database, "db");
        assert!(ep.connection_string.contains("sslmode=require"));
    }

    #[test]
    fn replication_apply_config_defaults() {
        let c = ReplicationApplyConfig::default();
        assert_eq!(c.feedback_interval, Duration::from_secs(10));
        assert_eq!(c.connection_timeout, Duration::from_secs(30));
        assert_eq!(c.health_check_interval, Duration::from_secs(60));
        assert!(c.max_runtime_seconds.is_none());
    }

    #[test]
    fn online_options_protocol_version_boundary() {
        let opts_zero = OnlineOptions {
            protocol_version: 0,
            ..OnlineOptions::default()
        };
        assert!(opts_zero.validate().is_err());

        let opts_five = OnlineOptions {
            protocol_version: 5,
            ..OnlineOptions::default()
        };
        assert!(opts_five.validate().is_err());

        let opts_four = OnlineOptions {
            protocol_version: 4,
            ..OnlineOptions::default()
        };
        assert!(opts_four.validate().is_ok());

        let opts_one = OnlineOptions {
            protocol_version: 1,
            ..OnlineOptions::default()
        };
        assert!(opts_one.validate().is_ok());
    }

    #[test]
    fn migration_config_deserializes_with_defaults_for_missing_fields() {
        let json = r#"{
            "mode": "Offline",
            "source": {"connection_string":"postgres://u@s/db","host":"s","port":5432,"database":"db","user":"u","password":""},
            "target": {"connection_string":"postgres://u@t/db","host":"t","port":5432,"database":"db","user":"u","password":""},
            "dump_scope": "All",
            "drop_target_first": false,
            "jobs": 4,
            "schemas": [],
            "tables": [],
            "online": {
                "slot_name": "slot",
                "publication": "pub",
                "protocol_version": 2,
                "subscription_name": "sub",
                "drop_subscription_on_cutover": true,
                "force_clean": false,
                "sync_sequences_on_cutover": true,
                "apply": {"feedback_interval":10,"connection_timeout":30,"health_check_interval":60,"max_runtime_seconds":null},
                "cutover": {"poll_interval":5,"fast_poll_interval":1,"lag_threshold_bytes":8192}
            },
            "allow_restore_errors": false,
            "verbose": false,
            "resume": false
        }"#;
        let cfg: MigrationConfig = serde_json::from_str(json).unwrap();
        assert!(cfg.no_publications);
        assert!(cfg.no_subscriptions);
        assert!(cfg.split_sections);
        assert_eq!(cfg.dump_compress, Some("lz4:1".to_string()));
        assert!(cfg.no_sync);
        assert!(cfg.no_comments);
        assert!(cfg.no_security_labels);
        assert!(!cfg.no_table_access_method);
        assert!(cfg.exclude_schemas.is_empty());
        assert!(cfg.exclude_tables.is_empty());
    }

    #[test]
    fn migration_config_serialize_with_none_compress() {
        let cfg = MigrationConfig {
            source: EndpointConfig::parse("postgres://u@s/db").unwrap(),
            target: EndpointConfig::parse("postgres://u@t/db").unwrap(),
            dump_compress: None,
            ..MigrationConfig::default()
        };
        let json = serde_json::to_string(&cfg).unwrap();
        assert!(json.contains("\"dump_compress\":null"));
        let cfg2: MigrationConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(cfg2.dump_compress, None);
    }

    #[test]
    fn endpoint_config_parse_url_encoded_password() {
        let ep = EndpointConfig::parse("postgresql://user:p%40ss%23w0rd@host/db").unwrap();
        assert_eq!(ep.password, "p%40ss%23w0rd");
        assert_eq!(ep.user, "user");
        assert_eq!(ep.host, "host");
    }

    #[test]
    fn endpoint_config_default_has_empty_fields() {
        let ep = EndpointConfig::default();
        assert!(ep.connection_string.is_empty());
        assert!(ep.host.is_empty());
        assert_eq!(ep.port, 0);
        assert!(ep.database.is_empty());
        assert!(ep.user.is_empty());
        assert!(ep.password.is_empty());
    }

    #[test]
    fn online_options_default_force_clean_is_false() {
        let opts = OnlineOptions::default();
        assert!(!opts.force_clean);
        assert!(opts.subscription_source_conn.is_none());
    }

    #[test]
    fn online_options_default_auto_create_publication_and_drop_slot() {
        let opts = OnlineOptions::default();
        assert!(opts.auto_create_publication);
        assert!(opts.drop_slot_on_cutover);
    }

    #[test]
    fn serde_backwards_compat_missing_auto_create_and_drop_slot_defaults_true() {
        let opts = OnlineOptions::default();
        let mut json: serde_json::Value = serde_json::to_value(&opts).unwrap();
        json.as_object_mut()
            .unwrap()
            .remove("auto_create_publication");
        json.as_object_mut().unwrap().remove("drop_slot_on_cutover");
        let opts2: OnlineOptions = serde_json::from_value(json).unwrap();
        assert!(opts2.auto_create_publication);
        assert!(opts2.drop_slot_on_cutover);
    }

    #[test]
    fn validate_accepts_valid_online_config() {
        let cfg = MigrationConfig {
            mode: MigrationMode::Online,
            source: EndpointConfig::parse("postgres://u@s/db").unwrap(),
            target: EndpointConfig::parse("postgres://u@t/db").unwrap(),
            ..MigrationConfig::default()
        };
        cfg.validate().unwrap();
    }

    #[test]
    fn serde_roundtrip_skip_analyze_fields() {
        let cfg = MigrationConfig {
            skip_analyze: true,
            skip_source_vacuum: true,
            source: EndpointConfig::parse("postgres://u@s/db").unwrap(),
            target: EndpointConfig::parse("postgres://u@t/db").unwrap(),
            ..MigrationConfig::default()
        };
        let json = serde_json::to_string(&cfg).unwrap();
        let cfg2: MigrationConfig = serde_json::from_str(&json).unwrap();
        assert!(cfg2.skip_analyze);
        assert!(cfg2.skip_source_vacuum);
    }

    #[test]
    fn migration_config_default_verify_is_warn() {
        let cfg = MigrationConfig::default();
        assert_eq!(cfg.verify, VerifyMode::Warn);
    }

    #[test]
    fn verify_mode_serde_roundtrip() {
        for mode in [VerifyMode::Off, VerifyMode::Warn, VerifyMode::Strict] {
            let json = serde_json::to_string(&mode).unwrap();
            let back: VerifyMode = serde_json::from_str(&json).unwrap();
            assert_eq!(back, mode);
        }
    }

    #[test]
    fn migration_mode_verify_serde_roundtrip() {
        let json = serde_json::to_string(&MigrationMode::Verify).unwrap();
        let back: MigrationMode = serde_json::from_str(&json).unwrap();
        assert_eq!(back, MigrationMode::Verify);
    }

    #[test]
    fn serde_backwards_compat_missing_skip_fields_default_false() {
        // Simulate a JSON config from an older version that doesn't have
        // the skip_analyze / skip_source_vacuum fields.
        let cfg = MigrationConfig {
            source: EndpointConfig::parse("postgres://u@s/db").unwrap(),
            target: EndpointConfig::parse("postgres://u@t/db").unwrap(),
            ..MigrationConfig::default()
        };
        let mut json: serde_json::Value = serde_json::to_value(&cfg).unwrap();
        json.as_object_mut().unwrap().remove("skip_analyze");
        json.as_object_mut().unwrap().remove("skip_source_vacuum");
        let cfg2: MigrationConfig = serde_json::from_value(json).unwrap();
        assert!(!cfg2.skip_analyze);
        assert!(!cfg2.skip_source_vacuum);
    }
}

#[cfg(test)]
mod split_copy_validation_tests {
    use super::*;

    fn split_cfg() -> MigrationConfig {
        MigrationConfig {
            source: EndpointConfig::parse("postgres://u:p@src/db").unwrap(),
            target: EndpointConfig::parse("postgres://u:p@dst/db").unwrap(),
            split_tables_larger_than: Some(1024),
            ..MigrationConfig::default()
        }
    }

    #[test]
    fn split_copy_off_by_default_and_validates_clean() {
        let cfg = MigrationConfig {
            source: EndpointConfig::parse("postgres://u:p@src/db").unwrap(),
            target: EndpointConfig::parse("postgres://u:p@dst/db").unwrap(),
            ..MigrationConfig::default()
        };
        assert!(cfg.split_tables_larger_than.is_none());
        assert!(cfg.validate().is_ok());
        assert!(split_cfg().validate().is_ok());
    }

    #[test]
    fn split_copy_rejects_zero_threshold_and_zero_parts() {
        let mut cfg = split_cfg();
        cfg.split_tables_larger_than = Some(0);
        assert!(format!("{}", cfg.validate().unwrap_err()).contains("must be > 0"));

        let mut cfg = split_cfg();
        cfg.split_max_parts = 0;
        assert!(format!("{}", cfg.validate().unwrap_err()).contains("split-max-parts"));
    }

    #[test]
    fn split_copy_rejects_schema_only_dump() {
        let mut cfg = split_cfg();
        cfg.dump_scope = DumpScope::SchemaOnly;
        assert!(format!("{}", cfg.validate().unwrap_err()).contains("schema-only"));
    }

    /// The planner queries the catalog directly instead of reimplementing
    /// pg_dump's pattern grammar, so any filter could put the two out of
    /// step — excluding a table's data from the archive while still copying
    /// it directly, or the reverse.
    #[test]
    fn split_copy_rejects_every_table_filter() {
        for mutate in [
            (|c: &mut MigrationConfig| c.schemas = vec!["app".into()]) as fn(&mut MigrationConfig),
            |c: &mut MigrationConfig| c.tables = vec!["app.t".into()],
            |c: &mut MigrationConfig| c.exclude_schemas = vec!["audit".into()],
            |c: &mut MigrationConfig| c.exclude_tables = vec!["app.big".into()],
        ] {
            let mut cfg = split_cfg();
            mutate(&mut cfg);
            let err = cfg.validate().unwrap_err();
            assert!(
                format!("{err}").contains("pattern matching"),
                "unexpected error: {err}"
            );
        }
    }

    /// An exported snapshot dies with the process that exported it, so a
    /// resumed run cannot read the split tables at the same instant as the
    /// archive it is resuming into.
    #[test]
    fn split_copy_rejects_resume() {
        let mut cfg = split_cfg();
        cfg.resume = true;
        cfg.dump_path = Some(std::path::PathBuf::from("/tmp/dump"));
        assert!(format!("{}", cfg.validate().unwrap_err()).contains("--resume"));
    }

    /// The all-in-one restore has already built indexes and foreign keys by
    /// the time the copy would run, so TRUNCATE fails outright on an
    /// FK-referenced table and the load would fire user triggers.
    #[test]
    fn split_copy_requires_split_sections() {
        let mut cfg = split_cfg();
        cfg.split_sections = false;
        assert!(format!("{}", cfg.validate().unwrap_err()).contains("split-section"));
    }
}
