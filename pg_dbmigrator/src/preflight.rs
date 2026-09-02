//! Pre-flight environment checks run before any migration work begins.
//!
//! These checks fail *fast and loudly* with actionable error messages, so the
//! operator can fix the environment before kicking off a multi-hour dump.

use std::io;
use std::process::ExitStatus;

use async_trait::async_trait;
use tracing::info;

use crate::config::MigrationMode;
use crate::error::{MigrationError, Result};
use crate::tls::connect_with_sslmode;

/// External tools that must be available on `$PATH` for the migrator to
/// function. `pg_dump` is required for both modes; `pg_restore` is required
/// for the restore phase.
pub const REQUIRED_TOOLS: &[&str] = &["pg_dump", "pg_restore"];

/// Snapshot of the target role's catalog state, used by
/// [`target_role_privilege_decision`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TargetRolePrivInfo {
    pub target_major: u32,
    pub has_create: bool,
    pub is_super: bool,
    pub has_sub_role: bool,
}

/// Abstracts the small set of process spawns and DB queries the preflight
/// async wrappers need. The real implementation is [`PgProbe`]; tests
/// supply a stub that returns predetermined values so the `verify_*_with_probe`
/// orchestration can be exercised without a live PostgreSQL or `pg_dump`.
#[async_trait]
pub(crate) trait PreflightProbe: Send + Sync {
    async fn capture_tool_version(&self, tool: &str) -> Result<String>;
    async fn server_major_version(&self, conn: &str) -> Result<u32>;
    async fn role_has_replication_or_super(&self, conn: &str) -> Result<bool>;
    async fn target_role_privilege_info(&self, conn: &str) -> Result<TargetRolePrivInfo>;
    async fn is_in_recovery(&self, conn: &str) -> Result<bool>;
    async fn subscription_capacity_gucs(&self, conn: &str) -> Result<(i64, i64, i64)>;
    /// Return the source's `idle_in_transaction_session_timeout` in
    /// milliseconds (0 means disabled).
    async fn idle_in_transaction_timeout_ms(&self, conn: &str) -> Result<i64>;
    /// Return `(source_installed, target_available)`: the extension names
    /// installed on the source (`pg_extension`) and the extension names
    /// available for installation on the target (`pg_available_extensions`).
    async fn installed_and_available_extensions(
        &self,
        source_conn: &str,
        target_conn: &str,
    ) -> Result<(Vec<String>, Vec<String>)>;
}

/// Production [`PreflightProbe`] — spawns real processes and opens real
/// PostgreSQL connections. Stateless; reuse a single value.
pub(crate) struct PgProbe;

#[async_trait]
impl PreflightProbe for PgProbe {
    async fn capture_tool_version(&self, tool: &str) -> Result<String> {
        capture_tool_version(tool).await
    }

    async fn server_major_version(&self, conn: &str) -> Result<u32> {
        server_major_version(conn).await
    }

    async fn role_has_replication_or_super(&self, conn: &str) -> Result<bool> {
        let client = connect_with_sslmode(conn).await?;
        // On AWS RDS the master user has neither `rolreplication` nor
        // `rolsuper` set directly; replication is granted via membership in
        // the `rds_replication` predefined role. The EXISTS subquery
        // short-circuits to false on non-RDS servers where `rds_replication`
        // doesn't exist, so this single query works everywhere.
        let row = client
            .query_one(
                "SELECT \
                   rolreplication OR rolsuper OR EXISTS ( \
                     SELECT 1 FROM pg_roles \
                     WHERE rolname = 'rds_replication' \
                       AND pg_has_role(current_user, oid, 'MEMBER') \
                   ) \
                 FROM pg_roles WHERE rolname = current_user",
                &[],
            )
            .await?;
        Ok(row.get(0))
    }

    async fn target_role_privilege_info(&self, conn: &str) -> Result<TargetRolePrivInfo> {
        let client = connect_with_sslmode(conn).await?;

        // Single round-trip: server version, CREATE privilege, superuser flag,
        // and pg_create_subscription membership in one query. The EXISTS
        // subquery against pg_roles short-circuits to false on pre-PG16
        // servers (where the predefined role does not exist), so the same
        // query works across all supported PostgreSQL versions.
        //
        // On PG16+, non-superusers need BOTH `pg_create_subscription`
        // membership AND the `CREATEDB` role attribute to actually run
        // CREATE SUBSCRIPTION — `has_sub_role` therefore requires both.
        let row = client
            .query_one(
                "SELECT \
                   current_setting('server_version_num')::integer AS svn, \
                   has_database_privilege(current_user, current_database(), 'CREATE') AS has_create, \
                   current_setting('is_superuser') = 'on' AS is_super, \
                   ( \
                     EXISTS ( \
                       SELECT 1 FROM pg_roles \
                       WHERE rolname = 'pg_create_subscription' \
                         AND pg_has_role(current_user, oid, 'MEMBER') \
                     ) \
                     AND (SELECT rolcreatedb FROM pg_roles WHERE rolname = current_user) \
                   ) AS has_sub_role",
                &[],
            )
            .await?;

        let svn: i32 = row.get("svn");
        let target_major: u32 = (svn / 10000) as u32;

        Ok(TargetRolePrivInfo {
            target_major,
            has_create: row.get("has_create"),
            is_super: row.get("is_super"),
            has_sub_role: row.get("has_sub_role"),
        })
    }

    async fn is_in_recovery(&self, conn: &str) -> Result<bool> {
        // Try the target DB first so we still work on managed PostgreSQL
        // services (Heroku, Render, etc.) where users typically lack
        // access to the `postgres` maintenance database. Fall back to the
        // maintenance DB only when the failure is specifically SQLSTATE
        // 3D000 ("invalid_catalog_name" / database does not exist) — on
        // network, firewall, or DNS errors we propagate immediately so we
        // don't double the connect timeout.
        let client = match connect_with_sslmode(conn).await {
            Ok(c) => c,
            Err(e) if is_undefined_database(&e) => {
                connect_with_sslmode(&maintenance_connection_string(conn)).await?
            }
            Err(e) => return Err(e),
        };
        let row = client.query_one("SELECT pg_is_in_recovery()", &[]).await?;
        Ok(row.get(0))
    }

    async fn subscription_capacity_gucs(&self, conn: &str) -> Result<(i64, i64, i64)> {
        let client = connect_with_sslmode(conn).await?;
        let row = client
            .query_one(
                "SELECT \
                   current_setting('max_logical_replication_workers')::integer, \
                   current_setting('max_worker_processes')::integer, \
                   current_setting('max_sync_workers_per_subscription')::integer",
                &[],
            )
            .await?;
        let a: i32 = row.get(0);
        let b: i32 = row.get(1);
        let c: i32 = row.get(2);
        Ok((a as i64, b as i64, c as i64))
    }

    async fn idle_in_transaction_timeout_ms(&self, conn: &str) -> Result<i64> {
        let client = connect_with_sslmode(conn).await?;
        // `pg_settings.setting` is the raw value in the GUC's base unit (ms
        // here). `current_setting()` would return a display string like
        // "10min", which does not cast to an integer.
        let row = client
            .query_one(
                "SELECT setting::bigint FROM pg_settings \
                 WHERE name = 'idle_in_transaction_session_timeout'",
                &[],
            )
            .await?;
        Ok(row.get(0))
    }

    async fn installed_and_available_extensions(
        &self,
        source_conn: &str,
        target_conn: &str,
    ) -> Result<(Vec<String>, Vec<String>)> {
        let (source, target) = tokio::try_join!(
            connect_with_sslmode(source_conn),
            connect_with_sslmode(target_conn),
        )?;
        let (installed_rows, available_rows) = tokio::try_join!(
            source.query("SELECT extname::text FROM pg_extension", &[]),
            target.query("SELECT name::text FROM pg_available_extensions", &[]),
        )?;
        let installed: Vec<String> = installed_rows.iter().map(|r| r.get(0)).collect();
        let available: Vec<String> = available_rows.iter().map(|r| r.get(0)).collect();
        Ok((installed, available))
    }
}

/// Outcome of a single preflight check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreflightOutcome {
    Pass,
    Skip { reason: &'static str },
}

/// Collected outcomes for a preflight bundle, used to emit a single
/// `MigrationStage::Validate` summary line for the operator.
#[derive(Debug, Default)]
pub struct PreflightReport {
    items: Vec<(&'static str, PreflightOutcome)>,
}

impl PreflightReport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&mut self, name: &'static str, outcome: PreflightOutcome) {
        self.items.push((name, outcome));
    }

    pub fn summary_line(&self) -> String {
        let pass = self
            .items
            .iter()
            .filter(|(_, o)| matches!(o, PreflightOutcome::Pass))
            .count();
        let skip = self
            .items
            .iter()
            .filter(|(_, o)| matches!(o, PreflightOutcome::Skip { .. }))
            .count();
        if self.items.is_empty() {
            return "preflight: 0 pass".to_string();
        }
        let names = self
            .items
            .iter()
            .map(|(n, o)| match o {
                PreflightOutcome::Pass => (*n).to_string(),
                PreflightOutcome::Skip { .. } => format!("{n}[skip]"),
            })
            .collect::<Vec<_>>()
            .join(", ");
        if skip == 0 {
            format!("preflight: {pass} pass ({names})")
        } else {
            format!("preflight: {pass} pass, {skip} skip ({names})")
        }
    }
}

/// Verify that every entry in [`REQUIRED_TOOLS`] is callable.
///
/// Returns the first missing tool as a [`MigrationError::MissingTool`]. On the
/// failure path only, the servers are asked what they actually run so the error
/// can name the major version to install instead of guessing one — the client
/// binaries are missing but the wire protocol still works. If that probe also
/// fails (unreachable server, bad credentials) the error is returned unchanged
/// and the generic hint on [`MigrationError::MissingTool`] carries the advice.
pub async fn verify_pg_tools_installed(source_conn: &str, target_conn: &str) -> Result<()> {
    for tool in REQUIRED_TOOLS {
        let outcome = spawn_version_check(tool).await;
        if let Err(err) = classify_version_check(tool, outcome) {
            let major = recommended_client_major(source_conn, target_conn)
                .await
                .ok();
            return Err(name_required_major(err, major));
        }
    }
    Ok(())
}

/// Fold a probed server major into a [`MigrationError::MissingTool`] reason. Pure, so the message is unit-testable without a live server.
pub(crate) fn name_required_major(err: MigrationError, major: Option<u32>) -> MigrationError {
    match (err, major) {
        (MigrationError::MissingTool { tool, reason }, Some(major)) => {
            MigrationError::missing_tool(
                tool,
                format!(
                    "{reason}; source and target run PostgreSQL {major}, \
                 so install the {major} client (e.g. `postgresql-client-{major}`)"
                ),
            )
        }
        (err, _) => err,
    }
}

/// Spawn `<tool> --version` with stdio silenced and return the raw outcome.
/// Split out so [`classify_version_check`] can be unit-tested without
/// actually spawning processes.
async fn spawn_version_check(tool: &str) -> std::result::Result<ExitStatus, io::Error> {
    use std::process::Stdio;
    use tokio::process::Command;
    Command::new(tool)
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
}

/// Pure interpretation of the version-check outcome — kept separate from the
/// spawn so it can be unit-tested deterministically.
pub(crate) fn classify_version_check(
    tool: &str,
    outcome: std::result::Result<ExitStatus, io::Error>,
) -> Result<()> {
    match outcome {
        Ok(s) if s.success() => Ok(()),
        Ok(s) => Err(MigrationError::missing_tool(
            tool,
            format!("`{tool} --version` exited with status {s}"),
        )),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let path = std::env::var("PATH").unwrap_or_default();
            Err(MigrationError::missing_tool(
                tool,
                format!("not found in $PATH (PATH={path})"),
            ))
        }
        Err(e) => Err(MigrationError::missing_tool(
            tool,
            format!("failed to spawn `{tool} --version`: {e}"),
        )),
    }
}

/// Parse the major version out of `pg_dump --version` / `pg_restore --version`
/// stdout. Accepts plain "16.4", Ubuntu-suffixed
/// "16.4 (Ubuntu 16.4-1.pgdg22.04+1)", and release-candidate "17rc1" forms.
/// Also tolerates custom builds (EnterpriseDB, Postgres Pro, etc.) that drop
/// the literal `(PostgreSQL)` marker by falling back to the first digit-led
/// whitespace token in the output.
///
/// Returns `None` if the input does not contain a recognizable version.
pub(crate) fn parse_pg_dump_version(stdout: &str) -> Option<u32> {
    // Preferred path: token immediately after the literal `(PostgreSQL)`
    // marker. Matching the marker (rather than just `)`) avoids false
    // positives from leading warnings or text that happens to contain
    // parenthesised groups (e.g. `warning: (ignored) ...`).
    if let Some(after) = stdout.split("(PostgreSQL)").nth(1) {
        if let Some(token) = after.split_whitespace().next() {
            let digits: String = token.chars().take_while(|c| c.is_ascii_digit()).collect();
            if let Ok(v) = digits.parse::<u32>() {
                return Some(v);
            }
        }
    }

    // Fallback for custom distributions (EnterpriseDB, Postgres Pro) that
    // omit the `(PostgreSQL)` marker — *or* for unusual formatting where
    // the token immediately after the marker isn't a version (e.g.
    // `(PostgreSQL) [custom] 16.4`). Narrow the search to the line that
    // looks like a version banner (contains "dump"/"restore"/"version")
    // so we don't pick up small numbers in leading warnings such as
    // "warning: 1 configuration file ignored".
    let scan_line = stdout
        .lines()
        .find(|line| {
            let l = line.to_lowercase();
            l.contains("dump") || l.contains("restore") || l.contains("version")
        })
        .unwrap_or(stdout);
    scan_line.split_whitespace().find_map(|t| {
        let leading: String = t.chars().take_while(|c| c.is_ascii_digit()).collect();
        leading.parse::<u32>().ok().filter(|v| *v < 100)
    })
}

/// Pure decision: `pg_dump` only reads from the source, so its major version
/// must be ≥ source server's major. It does **not** need to be ≥ target.
/// This lets common upgrade patterns work — e.g. PG14 → PG16 with pg_dump 14
/// + pg_restore 16 — which a single combined check would reject.
pub(crate) fn decide_pg_dump_compat(pg_dump_major: u32, source_major: u32) -> Result<()> {
    if pg_dump_major >= source_major {
        return Ok(());
    }
    Err(MigrationError::config(format!(
        "pg_dump is version {pg_dump_major}, but the source server is {source_major}. \
         pg_dump must be at least as new as the source server. \
         Install a matching client (e.g. `sudo apt install postgresql-client-{source_major}`) \
         or adjust $PATH so the newer binary is found first."
    )))
}

/// Pure decision: `pg_restore` writes to the target and reads the dump file,
/// so its major version must be ≥ target server's major *and* ≥ the
/// `pg_dump` version that produced the archive (newer dump formats can't be
/// read by older `pg_restore`).
pub(crate) fn decide_pg_restore_compat(
    pg_restore_major: u32,
    target_major: u32,
    pg_dump_major: u32,
) -> Result<()> {
    if pg_restore_major < target_major {
        return Err(MigrationError::config(format!(
            "pg_restore is version {pg_restore_major}, but the target server is {target_major}. \
             pg_restore must be at least as new as the target server. \
             Install a matching client (e.g. `sudo apt install postgresql-client-{target_major}`) \
             or adjust $PATH so the newer binary is found first."
        )));
    }
    if pg_restore_major < pg_dump_major {
        return Err(MigrationError::config(format!(
            "pg_restore is version {pg_restore_major}, but pg_dump is version {pg_dump_major}. \
             pg_restore must be at least as new as the pg_dump that produced the dump archive."
        )));
    }
    Ok(())
}

/// Spawn `<tool> --version` and return its stdout as UTF-8.
async fn capture_tool_version(tool: &str) -> Result<String> {
    use std::process::Stdio;
    use tokio::process::Command;
    let output = Command::new(tool)
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .await
        .map_err(|e| MigrationError::missing_tool(tool, format!("failed to spawn: {e}")))?;
    if !output.status.success() {
        return Err(MigrationError::missing_tool(
            tool,
            format!("`{tool} --version` exited {}", output.status),
        ));
    }
    String::from_utf8(output.stdout).map_err(|e| {
        MigrationError::config(format!("`{tool} --version` returned non-UTF8 output: {e}"))
    })
}

/// Read `server_version_num` from a live server and return the major version.
///
/// Tries the supplied connection first, then falls back to the maintenance
/// (`postgres`) database only on SQLSTATE 3D000 ("database does not
/// exist"). Other failures (network, firewall, DNS) propagate immediately
/// so we don't double the connect timeout.
async fn server_major_version(conn: &str) -> Result<u32> {
    let client = match connect_with_sslmode(conn).await {
        Ok(c) => c,
        Err(e) if is_undefined_database(&e) => {
            connect_with_sslmode(&maintenance_connection_string(conn)).await?
        }
        Err(e) => return Err(e),
    };
    let row = client
        .query_one("SELECT current_setting('server_version_num')::integer", &[])
        .await?;
    let num: i32 = row.get(0);
    // PG10+: 160004 -> major 16. PG9.x: 90624 -> major 9. Same formula works for both.
    Ok((num / 10000) as u32)
}

/// `true` if `err` wraps a tokio_postgres error with SQLSTATE `3D000`
/// (`invalid_catalog_name`), i.e. the target database does not exist.
/// Used to decide whether falling back to the maintenance DB is safe.
fn is_undefined_database(err: &MigrationError) -> bool {
    match err {
        MigrationError::Postgres(pg_err) => pg_err.code().map(|c| c.code()) == Some("3D000"),
        _ => false,
    }
}

/// Verify the local `pg_dump` (and `pg_restore`) binary major version is at
/// least as new as the newest server it will talk to. This catches the
/// common "I have pg_dump 14 but the source is PG16" footgun *before*
/// pg_dump runs and fails several minutes in.
pub async fn verify_pg_dump_version_compat(source_conn: &str, target_conn: &str) -> Result<()> {
    verify_pg_dump_version_compat_with_probe(&PgProbe, source_conn, target_conn).await
}

/// The PostgreSQL client major version that fits a source/target pair: the
/// newer of the two servers.
///
/// `pg_dump` must be at least as new as the source and `pg_restore` at least
/// as new as the target, so their maximum is the single client major that
/// satisfies both. `install.sh` reaches this through `--print-client-major` to
/// choose a `postgresql-client` package — over the wire protocol, because at
/// that point no client binary exists yet.
pub async fn recommended_client_major(source_conn: &str, target_conn: &str) -> Result<u32> {
    recommended_client_major_with_probe(&PgProbe, source_conn, target_conn).await
}

/// Probe-based variant of [`recommended_client_major`] — exposed at crate
/// level so unit tests can drive it with a stub probe.
pub(crate) async fn recommended_client_major_with_probe<P: PreflightProbe + ?Sized>(
    probe: &P,
    source_conn: &str,
    target_conn: &str,
) -> Result<u32> {
    let (source_major, target_major) = tokio::try_join!(
        probe.server_major_version(source_conn),
        probe.server_major_version(target_conn),
    )?;
    info!(source_major, target_major, "probed server versions");
    Ok(source_major.max(target_major))
}

/// Probe-based variant of [`verify_pg_dump_version_compat`] — exposed at
/// crate level so unit tests can drive it with a stub probe.
pub(crate) async fn verify_pg_dump_version_compat_with_probe<P: PreflightProbe + ?Sized>(
    probe: &P,
    source_conn: &str,
    target_conn: &str,
) -> Result<()> {
    // The four probes are independent — fan them out so total preflight
    // latency is bounded by the slowest one rather than the sum. The probe
    // itself falls back to the maintenance DB if the target DB doesn't yet
    // exist, so we can pass target_conn directly here.
    let (pg_dump_out, pg_restore_out, source_major, target_major) = tokio::try_join!(
        probe.capture_tool_version("pg_dump"),
        probe.capture_tool_version("pg_restore"),
        probe.server_major_version(source_conn),
        probe.server_major_version(target_conn),
    )?;
    let pg_dump_major = parse_pg_dump_version(&pg_dump_out).ok_or_else(|| {
        MigrationError::config(format!(
            "could not parse pg_dump version from `{pg_dump_out}`"
        ))
    })?;
    let pg_restore_major = parse_pg_dump_version(&pg_restore_out).ok_or_else(|| {
        MigrationError::config(format!(
            "could not parse pg_restore version from `{pg_restore_out}`"
        ))
    })?;
    decide_pg_dump_compat(pg_dump_major, source_major)?;
    decide_pg_restore_compat(pg_restore_major, target_major, pg_dump_major)?;
    info!(
        pg_dump_major,
        pg_restore_major,
        source_major,
        target_major,
        "pg_dump/pg_restore versions are compatible with source and target"
    );
    Ok(())
}

/// Confirm that a logical-replication publication with the given name exists
/// on the source. Run *before* `prepare_replication_slot` so the operator
/// gets an actionable error in seconds instead of waiting until the apply
/// worker dies on `CREATE SUBSCRIPTION`.
///
/// Returns [`MigrationError::Config`] when the publication is missing — the
/// operator must `CREATE PUBLICATION <name> FOR ALL TABLES` (or a more
/// targeted `FOR TABLE ...`) on the source before retrying.
pub async fn verify_publication_exists(source_conn: &str, publication: &str) -> Result<()> {
    let client = connect_with_sslmode(source_conn).await?;
    let row = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)",
            &[&publication],
        )
        .await?;
    let exists: bool = row.get(0);
    if !exists {
        return Err(MigrationError::config(format!(
            "publication `{publication}` does not exist on the source. \
             Run `CREATE PUBLICATION {publication} FOR ALL TABLES;` \
             (or a more targeted `FOR TABLE ...`) before retrying."
        )));
    }
    Ok(())
}

/// Confirm that the source server is configured for logical replication.
///
/// Online migrations *always* require `wal_level=logical` on the source — without it `CREATE_REPLICATION_SLOT` fails with a low-level libpq error several seconds into the run. This pre-flight produces a much better error: it points the operator at the exact GUC and reminds them a server restart is required.
///
/// Also opportunistically checks `max_replication_slots` and  `max_wal_senders` — both must be > 0 for any logical replication to work. A value of 0 (sometimes seen on freshly-spun managed PG instances) would otherwise show up as a confusing "all replication slots are in use" error.
pub async fn verify_source_logical_replication_ready(source_conn: &str) -> Result<()> {
    let client = connect_with_sslmode(source_conn).await?;

    // wal_level: must be 'logical'. 'replica' / 'minimal' won't work.
    let row = client
        .query_one("SELECT current_setting('wal_level')", &[])
        .await?;
    let wal_level: String = row.get(0);
    if wal_level != "logical" {
        return Err(MigrationError::config(format!(
            "the source server has `wal_level = '{wal_level}'`. \
             Online migrations require `wal_level = 'logical'`. \
             Set it via `ALTER SYSTEM SET wal_level = 'logical';` \
             and restart the source server (this GUC is not reloadable)."
        )));
    }

    // max_replication_slots / max_wal_senders: ensure they are > 0.
    // current_setting() returns the value as text; integer GUCs are
    // safe to parse with FromStr.
    for guc in ["max_replication_slots", "max_wal_senders"] {
        let row = client
            .query_one("SELECT current_setting($1)::text", &[&guc])
            .await?;
        let raw: String = row.get(0);
        let parsed: i64 = raw.trim().parse().map_err(|_| {
            MigrationError::config(format!(
                "could not parse `{guc}` value `{raw}` as an integer"
            ))
        })?;
        if parsed <= 0 {
            return Err(MigrationError::config(format!(
                "the source server has `{guc} = {parsed}`. \
                 Online migrations require `{guc} > 0`. \
                 Raise it (PostgreSQL recommends >= 4) and restart \
                 the source server."
            )));
        }
    }

    info!("source is configured for logical replication (wal_level=logical)");
    Ok(())
}

/// Verify the source role used by pg_dbmigrator can stream replication.
/// Either the explicit `REPLICATION` attribute or `rolsuper` is sufficient —
/// superusers bypass the `rolreplication` check inside the server. Without
/// one of those, `CREATE_REPLICATION_SLOT` fails seconds into the run with
/// a libpq-level error.
///
/// Online mode only — offline migrations never open a replication connection.
pub async fn verify_source_replication_role(source_conn: &str) -> Result<()> {
    verify_source_replication_role_with_probe(&PgProbe, source_conn).await
}

/// Probe-based variant of [`verify_source_replication_role`].
pub(crate) async fn verify_source_replication_role_with_probe<P: PreflightProbe + ?Sized>(
    probe: &P,
    source_conn: &str,
) -> Result<()> {
    let has_repl = probe.role_has_replication_or_super(source_conn).await?;
    if !has_repl {
        return Err(MigrationError::config(
            "the source role lacks the REPLICATION attribute (and is not a superuser), \
             which is required to create a logical replication slot. \
             Fix on the source: `ALTER ROLE \"<your_user>\" REPLICATION;` (must be \
             run by a superuser)."
                .to_string(),
        ));
    }
    info!("source role can replicate (REPLICATION attribute or superuser)");
    Ok(())
}

/// Quote a potentially schema-qualified name (`schema.table`) by splitting
/// on `.` and quoting each part individually. Unqualified names (no dot)
/// are quoted as a single identifier.
///
/// PostgreSQL requires `"schema"."table"` — quoting the whole thing as
/// `"schema.table"` creates a single identifier that includes a literal dot.
pub fn quote_qualified_name(name: &str) -> Result<String> {
    let parts: Vec<&str> = name.splitn(2, '.').collect();
    if parts.iter().any(|p| p.is_empty()) {
        return Err(MigrationError::config(format!(
            "invalid qualified name: `{name}` (empty component)"
        )));
    }
    let quoted: std::result::Result<Vec<_>, _> =
        parts.iter().map(|p| pg_walstream::quote_ident(p)).collect();
    Ok(quoted?.join("."))
}

/// Build the `CREATE PUBLICATION` SQL statement from the given parameters.
///
/// When both `tables` and `schemas` are empty, creates `FOR ALL TABLES`.
/// When `tables` is non-empty, creates `FOR TABLE <t1>, <t2>, …`.
/// When only `schemas` is non-empty, creates `FOR TABLES IN SCHEMA <s1>, <s2>, …`.
pub fn build_create_publication_sql(
    publication: &str,
    tables: &[String],
    schemas: &[String],
) -> Result<String> {
    let pub_ident = pg_walstream::quote_ident(publication)?;
    let scope = if !tables.is_empty() || !schemas.is_empty() {
        let mut scope_parts = Vec::new();
        if !tables.is_empty() {
            let quoted: std::result::Result<Vec<_>, _> =
                tables.iter().map(|t| quote_qualified_name(t)).collect();
            scope_parts.push(format!("TABLE {}", quoted?.join(", ")));
        }
        if !schemas.is_empty() {
            let quoted: std::result::Result<Vec<_>, _> = schemas
                .iter()
                .map(|s| pg_walstream::quote_ident(s))
                .collect();
            scope_parts.push(format!("TABLES IN SCHEMA {}", quoted?.join(", ")));
        }
        format!("FOR {}", scope_parts.join(", "))
    } else {
        "FOR ALL TABLES".to_string()
    };
    Ok(format!("CREATE PUBLICATION {pub_ident} {scope}"))
}

/// Filter a list of `schema.table` names by removing entries that match
/// `exclude_tables` or belong to a schema in `exclude_schemas`.
pub fn filter_tables_by_exclusions(
    tables: &[String],
    exclude_tables: &[String],
    exclude_schemas: &[String],
) -> Vec<String> {
    tables
        .iter()
        .filter(|t| {
            if exclude_tables.iter().any(|ex| ex == *t) {
                return false;
            }
            if let Some(schema) = t.split('.').next() {
                if exclude_schemas.iter().any(|ex| ex == schema) {
                    return false;
                }
            }
            true
        })
        .cloned()
        .collect()
}

/// Query the source for all ordinary and partitioned tables, excluding
/// system schemas and applying the caller's exclusion lists. Returns
/// `schema.table` qualified names suitable for `build_create_publication_sql`.
async fn fetch_published_tables(
    client: &tokio_postgres::Client,
    exclude_tables: &[String],
    exclude_schemas: &[String],
) -> Result<Vec<String>> {
    let rows = client
        .query(
            "SELECT n.nspname::text, c.relname::text \
             FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relkind IN ('r', 'p') \
               AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') \
               AND n.nspname NOT LIKE 'pg_temp_%' \
               AND n.nspname NOT LIKE 'pg_toast_temp_%'",
            &[],
        )
        .await?;

    let all_tables: Vec<String> = rows
        .iter()
        .map(|r| {
            let schema: &str = r.get(0);
            let table: &str = r.get(1);
            format!("{schema}.{table}")
        })
        .collect();

    Ok(filter_tables_by_exclusions(
        &all_tables,
        exclude_tables,
        exclude_schemas,
    ))
}

/// Ensure that a logical-replication publication with the given name exists
/// on the source. If absent and `auto_create` is enabled, create it
/// automatically.
///
/// When `exclude_tables` or `exclude_schemas` are non-empty and the include
/// lists (`tables`, `schemas`) are empty, the publication is scoped to an
/// explicit table list obtained by querying the source and subtracting the
/// excluded objects. This prevents the target's apply worker from crashing
/// when excluded objects are modified on the source.
///
/// Returns `Ok(true)` if the publication was auto-created, `Ok(false)` if
/// it already existed.
pub async fn ensure_publication_exists(
    source_conn: &str,
    publication: &str,
    tables: &[String],
    schemas: &[String],
    exclude_tables: &[String],
    exclude_schemas: &[String],
) -> Result<bool> {
    let client = connect_with_sslmode(source_conn).await?;
    let row = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)",
            &[&publication],
        )
        .await?;
    let exists: bool = row.get(0);
    if exists {
        info!(publication, "publication already exists on source");
        return Ok(false);
    }

    let has_exclusions = !exclude_tables.is_empty() || !exclude_schemas.is_empty();
    let has_includes = !tables.is_empty() || !schemas.is_empty();

    let (effective_tables, effective_schemas): (Vec<String>, Vec<String>) = if has_exclusions
        && !has_includes
    {
        let resolved = fetch_published_tables(&client, exclude_tables, exclude_schemas).await?;
        (resolved, Vec::new())
    } else if has_exclusions && has_includes {
        let filtered_tables = filter_tables_by_exclusions(tables, exclude_tables, exclude_schemas);
        let filtered_schemas: Vec<String> = schemas
            .iter()
            .filter(|s| !exclude_schemas.iter().any(|ex| ex == *s))
            .cloned()
            .collect();
        (filtered_tables, filtered_schemas)
    } else {
        (tables.to_vec(), schemas.to_vec())
    };

    let sql = build_create_publication_sql(publication, &effective_tables, &effective_schemas)?;
    info!(publication, sql = %sql, "auto-creating publication on source");
    client.batch_execute(&sql).await?;
    info!(publication, "publication created successfully");
    Ok(true)
}

/// Rewrite a connection string so the path component (database name) points
/// to the `postgres` maintenance database. Used to run admin commands like
/// `CREATE DATABASE` which cannot target the database they are creating.
pub fn maintenance_connection_string(conn: &str) -> String {
    match conn.find('?') {
        Some(q) => {
            let scheme_end = conn.find("://").map(|i| i + 3).unwrap_or(0);
            let at = conn[scheme_end..q].rfind('@').map(|i| i + scheme_end);
            let host_start = at.map(|i| i + 1).unwrap_or(scheme_end);
            // Find first '/' after host:port — that starts the db name.
            match conn[host_start..q].find('/') {
                Some(slash) => {
                    let abs = host_start + slash;
                    format!("{}/postgres{}", &conn[..abs], &conn[q..])
                }
                None => conn.to_string(),
            }
        }
        None => {
            let scheme_end = conn.find("://").map(|i| i + 3).unwrap_or(0);
            let at = conn[scheme_end..].rfind('@').map(|i| i + scheme_end);
            let host_start = at.map(|i| i + 1).unwrap_or(scheme_end);
            match conn[host_start..].find('/') {
                Some(slash) => {
                    let abs = host_start + slash;
                    format!("{}/postgres", &conn[..abs])
                }
                None => conn.to_string(),
            }
        }
    }
}

/// Ensure the target database exists, creating it if necessary.
///
/// Connects to the `postgres` maintenance database on the target server and
/// checks `pg_database`. If the target database is missing, issues
/// `CREATE DATABASE`. This runs early in the online pipeline — before
/// `pg_restore` needs a live target database.
pub async fn ensure_target_database_exists(target_conn: &str, db_name: &str) -> Result<()> {
    let maint_conn = maintenance_connection_string(target_conn);
    let client = connect_with_sslmode(&maint_conn).await?;
    let row = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)",
            &[&db_name],
        )
        .await?;
    let exists: bool = row.get(0);
    if exists {
        info!(database = db_name, "target database already exists");
    } else {
        info!(database = db_name, "creating target database");
        let create_sql = format!("CREATE DATABASE {}", pg_walstream::quote_ident(db_name)?);
        client.batch_execute(&create_sql).await?;
        info!(database = db_name, "target database created");
    }
    Ok(())
}

/// Check whether `pglogical` is loaded in `shared_preload_libraries` on the
/// target and warn/error if so.
///
/// **Background**: when `pglogical` is a shared preload library it installs
/// hooks into the logical-replication launcher that prevent native
/// `CREATE SUBSCRIPTION` apply workers from starting. The workers launch,
/// immediately crash, and never connect to the source — leaving replication
/// permanently stalled with no useful error message in `pg_stat_subscription`.
///
/// This function detects the situation early and fails with an actionable
/// message so the operator can remove `pglogical` from
/// `shared_preload_libraries` (and restart the server) before proceeding.
///
/// On vanilla PostgreSQL (or any server where `pglogical` is not preloaded)
/// the check is a silent no-op.
pub async fn ensure_pglogical_not_interfering(target_conn: &str) -> Result<()> {
    let client = connect_with_sslmode(target_conn).await?;

    let row = client
        .query_one("SELECT current_setting('shared_preload_libraries')", &[])
        .await?;
    let libs: &str = row.get(0);

    if libs.split(',').any(|lib| lib.trim() == "pglogical") {
        return Err(MigrationError::config(
            "the target server has `pglogical` in `shared_preload_libraries`. \
             This is known to prevent native PostgreSQL logical-replication apply \
             workers from starting (the workers crash silently on launch). \
             Remove `pglogical` from `shared_preload_libraries` and restart the \
             server before retrying."
                .to_string(),
        ));
    }

    info!("pglogical is not in shared_preload_libraries — native logical replication will work");
    Ok(())
}

/// Pure decision: does the target role have the privileges needed for the
/// given migration mode on a server of `target_major`?
///
/// - **Offline**: needs `CREATE` on the target database (for schema restore).
/// - **Online**: same, plus the ability to create a subscription. PG16+
///   exposes the `pg_create_subscription` predefined role; pre-PG16 there
///   is no such role and the user must be a superuser.
pub(crate) fn target_role_privilege_decision(
    mode: MigrationMode,
    target_major: u32,
    has_create_on_db: bool,
    is_superuser: bool,
    has_pg_create_subscription_role: bool,
) -> Result<()> {
    if !has_create_on_db && !is_superuser {
        return Err(MigrationError::config(
            "the target role lacks CREATE privilege on the target database. \
             Grant it on the target: \
             `GRANT CREATE ON DATABASE \"<db>\" TO \"<user>\";`"
                .to_string(),
        ));
    }
    if matches!(mode, MigrationMode::Online) {
        if target_major >= 16 {
            if !is_superuser && !has_pg_create_subscription_role {
                return Err(MigrationError::config(
                    "the target role cannot create subscriptions. \
                     On the target: `GRANT pg_create_subscription TO \"<user>\"; \
                     ALTER ROLE \"<user>\" CREATEDB;` \
                     (or make the role a superuser)."
                        .to_string(),
                ));
            }
        } else if !is_superuser {
            return Err(MigrationError::config(
                "the target server is pre-PG16 and online migration requires a \
                 superuser role on the target (the `pg_create_subscription` \
                 predefined role does not exist before PG16). \
                 Either run as a superuser or upgrade the target to PG16+."
                    .to_string(),
            ));
        }
    }
    Ok(())
}

/// Verify the target server is writable — i.e. it is not a hot standby.
///
/// pg_dbmigrator needs to `CREATE DATABASE`, `pg_restore`, and (online)
/// `CREATE SUBSCRIPTION` on the target; a server in recovery rejects all
/// writes with `cannot execute … in a read-only transaction`.
pub async fn verify_target_not_in_recovery(target_conn: &str) -> Result<()> {
    verify_target_not_in_recovery_with_probe(&PgProbe, target_conn).await
}

/// Probe-based variant of [`verify_target_not_in_recovery`].
pub(crate) async fn verify_target_not_in_recovery_with_probe<P: PreflightProbe + ?Sized>(
    probe: &P,
    target_conn: &str,
) -> Result<()> {
    let in_recovery = probe.is_in_recovery(target_conn).await?;
    if in_recovery {
        return Err(MigrationError::config(
            "the target server is a hot standby (in recovery). pg_dbmigrator \
             requires a writable target. Promote the standby first (`pg_ctl promote` \
             or `SELECT pg_promote();`) or point --target at a primary."
                .to_string(),
        ));
    }
    info!("target server is a primary (not in recovery)");
    Ok(())
}

/// Pure decision: can a snapshot survive an entire `pg_dump` on a source
/// with this `idle_in_transaction_session_timeout`?
///
/// It cannot. The connection that runs `CREATE_REPLICATION_SLOT ...
/// EXPORT_SNAPSHOT` must stay open for the whole dump to keep the exported
/// snapshot alive, and it is genuinely idle-in-transaction that entire
/// time — `START_REPLICATION` is deliberately deferred until after the
/// dump. PostgreSQL terminates it with `FATAL: terminating connection due
/// to idle-in-transaction timeout`, and the dump then fails against a dead
/// snapshot, typically hours in.
///
/// The GUC cannot be overridden from our side: `pg_walstream` parses the
/// conninfo itself and ignores libpq's `options`, so there is no way to
/// push a session setting down onto that connection. Failing here, before
/// any work happens, is the only thing left.
pub(crate) fn classify_snapshot_timeout(idle_timeout_ms: i64) -> Result<()> {
    if idle_timeout_ms == 0 {
        return Ok(());
    }
    Err(MigrationError::config(format!(
        "the source has idle_in_transaction_session_timeout={idle_timeout_ms}ms, which will \
         terminate the connection holding the exported snapshot part-way through pg_dump and \
         fail the migration. Disable it for the migration role \
         (`ALTER ROLE <user> SET idle_in_transaction_session_timeout = 0;`) or server-wide \
         (`ALTER SYSTEM SET idle_in_transaction_session_timeout = 0; SELECT pg_reload_conf();`). \
         Offline mode without --split-tables-larger-than takes no exported snapshot and is \
         unaffected."
    )))
}

/// Check that the source will not kill the snapshot-holding connection
/// mid-dump.
///
/// Always run for online mode, which holds the replication slot's exported
/// snapshot across the dump. Also run for offline mode when
/// `--split-tables-larger-than` is set, because the split copy exports its
/// own snapshot and holds it just as long.
pub async fn verify_source_snapshot_timeouts(source_conn: &str) -> Result<()> {
    verify_source_snapshot_timeouts_with_probe(&PgProbe, source_conn).await
}

/// Probe-based variant of [`verify_source_snapshot_timeouts`].
pub(crate) async fn verify_source_snapshot_timeouts_with_probe<P: PreflightProbe + ?Sized>(
    probe: &P,
    source_conn: &str,
) -> Result<()> {
    let idle_ms = probe.idle_in_transaction_timeout_ms(source_conn).await?;
    classify_snapshot_timeout(idle_ms)?;
    info!("source idle_in_transaction_session_timeout is disabled; snapshot can outlive pg_dump");
    Ok(())
}

/// Minimum acceptable target-side subscription-related GUC values. These
/// are the absolute floor required for `CREATE SUBSCRIPTION` to function
/// at all — one logical-replication worker and two worker-process slots.
/// pg_dbmigrator deliberately does **not** enforce the higher production
/// recommendations (4/8) so we don't block migrations on resource-
/// constrained targets like local Docker containers, CI runners, or small
/// managed instances (e.g. RDS micro/nano tiers). Operators running large
/// migrations should still raise these on the target for throughput.
pub(crate) const RECOMMENDED_LRW: i64 = 1;
pub(crate) const RECOMMENDED_MWP: i64 = 2;

/// Pure decision: does the target have enough worker headroom to run our
/// `CREATE SUBSCRIPTION` apply path? Each GUC is checked independently so
/// the error message names the exact knob that needs raising.
pub(crate) fn classify_subscription_capacity(
    max_logical_replication_workers: i64,
    max_worker_processes: i64,
    _max_sync_workers_per_subscription: i64,
) -> Result<()> {
    if max_logical_replication_workers < RECOMMENDED_LRW {
        return Err(MigrationError::config(format!(
            "the target has max_logical_replication_workers={max_logical_replication_workers}, \
             but pg_dbmigrator requires >= {RECOMMENDED_LRW}. \
             `ALTER SYSTEM SET max_logical_replication_workers = {RECOMMENDED_LRW};` and restart \
             the target."
        )));
    }
    if max_worker_processes < RECOMMENDED_MWP {
        return Err(MigrationError::config(format!(
            "the target has max_worker_processes={max_worker_processes}, but pg_dbmigrator \
             requires >= {RECOMMENDED_MWP}. \
             `ALTER SYSTEM SET max_worker_processes = {RECOMMENDED_MWP};` and restart the target."
        )));
    }
    Ok(())
}

/// Verify the target has enough logical-replication worker headroom for
/// `CREATE SUBSCRIPTION` to make forward progress.
pub async fn verify_target_subscription_capacity(target_conn: &str) -> Result<()> {
    verify_target_subscription_capacity_with_probe(&PgProbe, target_conn).await
}

/// Probe-based variant of [`verify_target_subscription_capacity`].
pub(crate) async fn verify_target_subscription_capacity_with_probe<P: PreflightProbe + ?Sized>(
    probe: &P,
    target_conn: &str,
) -> Result<()> {
    let (max_logical_replication_workers, max_worker_processes, max_sync_workers_per_subscription) =
        probe.subscription_capacity_gucs(target_conn).await?;
    classify_subscription_capacity(
        max_logical_replication_workers,
        max_worker_processes,
        max_sync_workers_per_subscription,
    )?;
    info!(
        max_logical_replication_workers,
        max_worker_processes,
        max_sync_workers_per_subscription,
        "target has sufficient subscription worker capacity"
    );
    Ok(())
}

/// Verify the target role has the privileges needed for the chosen mode.
pub async fn verify_target_role_privileges(target_conn: &str, mode: MigrationMode) -> Result<()> {
    verify_target_role_privileges_with_probe(&PgProbe, target_conn, mode).await
}

/// Probe-based variant of [`verify_target_role_privileges`].
pub(crate) async fn verify_target_role_privileges_with_probe<P: PreflightProbe + ?Sized>(
    probe: &P,
    target_conn: &str,
    mode: MigrationMode,
) -> Result<()> {
    let info = probe.target_role_privilege_info(target_conn).await?;
    target_role_privilege_decision(
        mode,
        info.target_major,
        info.has_create,
        info.is_super,
        info.has_sub_role,
    )?;
    info!(target_major = info.target_major, mode = ?mode, "target role has required privileges");
    Ok(())
}

/// Pure decision: every extension installed on the source must be available
/// (installable) on the target, else `pg_restore`'s `CREATE EXTENSION` fails
/// partway through the restore. Returns the missing extension names in the
/// error so the operator can install the matching `-contrib` package.
pub(crate) fn decide_extensions_available(
    source_installed: &[String],
    target_available: &[String],
) -> Result<()> {
    let available: std::collections::HashSet<&str> =
        target_available.iter().map(|s| s.as_str()).collect();
    let missing: Vec<&str> = source_installed
        .iter()
        .map(|s| s.as_str())
        .filter(|e| !available.contains(e))
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    Err(MigrationError::config(format!(
        "the source has extension(s) not available on the target: {}. \
         Install the matching package(s) on the target (e.g. \
         `postgresql-<major>-<ext>` / the extension's contrib package) so \
         `pg_restore`'s CREATE EXTENSION succeeds, or exclude the objects \
         that depend on them.",
        missing.join(", ")
    )))
}

/// Verify every source-installed extension is installable on the target.
pub async fn verify_target_extensions_available(
    source_conn: &str,
    target_conn: &str,
) -> Result<()> {
    verify_target_extensions_available_with_probe(&PgProbe, source_conn, target_conn).await
}

/// Probe-based variant of [`verify_target_extensions_available`] — exposed at
/// crate level so unit tests can drive it with a stub probe. Fetches the
/// source-installed and target-available extension names via the probe, then
/// delegates the (pure) comparison to [`decide_extensions_available`].
pub(crate) async fn verify_target_extensions_available_with_probe<P: PreflightProbe + ?Sized>(
    probe: &P,
    source_conn: &str,
    target_conn: &str,
) -> Result<()> {
    let (installed, available) = probe
        .installed_and_available_extensions(source_conn, target_conn)
        .await?;
    decide_extensions_available(&installed, &available)?;
    info!("all source extensions are available on the target");
    Ok(())
}

/// Resolve the extension-availability preflight outcome. On success →
/// Pass. On failure, if `allow_restore_errors` is set the missing
/// extensions are non-fatal (the user opted into tolerating un-portable
/// extension state) → warn + Skip; otherwise the error propagates.
pub(crate) fn resolve_extension_preflight(
    result: Result<()>,
    allow_restore_errors: bool,
) -> Result<PreflightOutcome> {
    match result {
        Ok(()) => Ok(PreflightOutcome::Pass),
        Err(MigrationError::Config(msg)) if allow_restore_errors => {
            tracing::warn!(error = %msg, "some source extensions are not available on the target (continuing because --allow-restore-errors is set)");
            Ok(PreflightOutcome::Skip {
                reason: "missing extensions allowed by --allow-restore-errors",
            })
        }
        Err(e) => Err(e),
    }
}

impl PreflightReport {
    pub fn names(&self) -> Vec<&'static str> {
        self.items.iter().map(|(n, _)| *n).collect()
    }
}

/// Static list of check names for the offline bundle, in order. Exposed so
/// callers/tests can assert bundle composition without running the bundle.
///
/// One check is conditional and therefore absent here: when
/// `--split-tables-larger-than` is set, the offline bundle additionally runs
/// `source_snapshot_timeouts`, because the split copy exports a snapshot the
/// plain offline path never takes.
///
/// Ordering invariant: every check that opens a connection to the *target
/// database itself* (as opposed to the maintenance `postgres` DB) must run
/// after `target_db_exists`, since the target DB may not yet exist on a
/// first-run offline migration.
pub fn offline_preflight_check_names() -> &'static [&'static str] {
    &[
        "pg_tools",
        "version_compat",
        "target_not_in_recovery",
        "target_db_exists",
        "target_role_privs",
        "extensions_available",
    ]
}

/// Static list of check names for the online bundle, in order.
///
/// Same ordering invariant as [`offline_preflight_check_names`]: checks that
/// open the target DB directly run after `target_db_exists`.
pub fn online_preflight_check_names() -> &'static [&'static str] {
    &[
        "pg_tools",
        "version_compat",
        "source_repl_role",
        "source_snapshot_timeouts",
        "target_not_in_recovery",
        "target_db_exists",
        "target_role_privs",
        "extensions_available",
        "source_logical_repl",
        "target_sub_capacity",
        "pglogical_clean",
    ]
}

/// Run the offline preflight bundle. Fail-fast on the first failing check;
/// otherwise return a `PreflightReport` for summary logging.
pub async fn run_offline_preflight(
    cfg: &crate::config::MigrationConfig,
) -> Result<PreflightReport> {
    let mut report = PreflightReport::new();

    verify_pg_tools_installed(&cfg.source.connection_string, &cfg.target.connection_string).await?;
    report.record("pg_tools", PreflightOutcome::Pass);

    verify_pg_dump_version_compat(&cfg.source.connection_string, &cfg.target.connection_string)
        .await?;
    report.record("version_compat", PreflightOutcome::Pass);

    // verify_target_not_in_recovery uses the maintenance DB internally, so
    // it is safe to call before the target database has been created.
    verify_target_not_in_recovery(&cfg.target.connection_string).await?;
    report.record("target_not_in_recovery", PreflightOutcome::Pass);

    // Create the target DB if missing before any check that connects to it
    // directly (e.g. has_database_privilege only works on a DB that exists).
    ensure_target_database_exists(&cfg.target.connection_string, &cfg.target.database).await?;
    report.record("target_db_exists", PreflightOutcome::Pass);

    verify_target_role_privileges(&cfg.target.connection_string, MigrationMode::Offline).await?;
    report.record("target_role_privs", PreflightOutcome::Pass);

    // Only relevant when the split copy is on: that path exports its own
    // snapshot and holds it idle-in-transaction for the whole dump and
    // restore, exactly like the online slot connection does.
    if cfg.split_tables_larger_than.is_some() {
        verify_source_snapshot_timeouts(&cfg.source.connection_string).await?;
        report.record("source_snapshot_timeouts", PreflightOutcome::Pass);
    }

    let ext_outcome = resolve_extension_preflight(
        verify_target_extensions_available(
            &cfg.source.connection_string,
            &cfg.target.connection_string,
        )
        .await,
        cfg.allow_restore_errors,
    )?;
    report.record("extensions_available", ext_outcome);

    Ok(report)
}

/// Run the online preflight bundle. Fail-fast on the first failing check.
pub async fn run_online_preflight(cfg: &crate::config::MigrationConfig) -> Result<PreflightReport> {
    let mut report = PreflightReport::new();

    verify_pg_tools_installed(&cfg.source.connection_string, &cfg.target.connection_string).await?;
    report.record("pg_tools", PreflightOutcome::Pass);

    verify_pg_dump_version_compat(&cfg.source.connection_string, &cfg.target.connection_string)
        .await?;
    report.record("version_compat", PreflightOutcome::Pass);

    verify_source_replication_role(&cfg.source.connection_string).await?;
    report.record("source_repl_role", PreflightOutcome::Pass);

    // Must run before the dump: an idle-in-transaction timeout on the source
    // kills the snapshot holder part-way through pg_dump, so catching it here
    // saves the operator hours of work that is already doomed.
    verify_source_snapshot_timeouts(&cfg.source.connection_string).await?;
    report.record("source_snapshot_timeouts", PreflightOutcome::Pass);

    // target_not_in_recovery uses the maintenance DB; safe before target_db_exists.
    verify_target_not_in_recovery(&cfg.target.connection_string).await?;
    report.record("target_not_in_recovery", PreflightOutcome::Pass);

    // Create target DB before any check that connects to it directly.
    ensure_target_database_exists(&cfg.target.connection_string, &cfg.target.database).await?;
    report.record("target_db_exists", PreflightOutcome::Pass);

    verify_target_role_privileges(&cfg.target.connection_string, MigrationMode::Online).await?;
    report.record("target_role_privs", PreflightOutcome::Pass);

    let ext_outcome = resolve_extension_preflight(
        verify_target_extensions_available(
            &cfg.source.connection_string,
            &cfg.target.connection_string,
        )
        .await,
        cfg.allow_restore_errors,
    )?;
    report.record("extensions_available", ext_outcome);

    verify_source_logical_replication_ready(&cfg.source.connection_string).await?;
    report.record("source_logical_repl", PreflightOutcome::Pass);

    verify_target_subscription_capacity(&cfg.target.connection_string).await?;
    report.record("target_sub_capacity", PreflightOutcome::Pass);

    ensure_pglogical_not_interfering(&cfg.target.connection_string).await?;
    report.record("pglogical_clean", PreflightOutcome::Pass);

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::process::ExitStatusExt;

    #[test]
    fn extensions_ok_when_all_available_on_target() {
        let src = vec!["plpgsql".to_string(), "postgis".to_string()];
        let tgt = vec![
            "plpgsql".to_string(),
            "postgis".to_string(),
            "hstore".to_string(),
        ];
        assert!(decide_extensions_available(&src, &tgt).is_ok());
    }

    #[test]
    fn extensions_error_names_missing_extension() {
        let src = vec!["plpgsql".to_string(), "timescaledb".to_string()];
        let tgt = vec!["plpgsql".to_string()];
        let err = decide_extensions_available(&src, &tgt).unwrap_err();
        match err {
            MigrationError::Config(msg) => assert!(msg.contains("timescaledb")),
            other => panic!("expected Config, got {other:?}"),
        }
    }

    #[test]
    fn resolve_extension_returns_pass_when_ok() {
        let outcome = resolve_extension_preflight(Ok(()), false).unwrap();
        assert_eq!(outcome, PreflightOutcome::Pass);
    }

    #[test]
    fn resolve_extension_returns_skip_when_error_and_allowed() {
        let result = Err(MigrationError::config("missing ext foo"));
        let outcome = resolve_extension_preflight(result, true).unwrap();
        match outcome {
            PreflightOutcome::Skip { reason } => {
                assert!(reason.contains("--allow-restore-errors"));
            }
            other => panic!("expected Skip, got {other:?}"),
        }
    }

    #[test]
    fn resolve_extension_returns_err_when_error_and_not_allowed() {
        let result = Err(MigrationError::config("missing ext foo"));
        assert!(resolve_extension_preflight(result, false).is_err());
    }

    #[test]
    fn resolve_extension_propagates_non_config_error_even_when_allowed() {
        // A fatal (non-Config) error such as a connection/query failure must
        // NOT be swallowed by --allow-restore-errors; only missing-extension
        // (Config) errors are skippable.
        let result = Err(MigrationError::apply("boom"));
        assert!(resolve_extension_preflight(result, true).is_err());
    }

    fn ok_status() -> ExitStatus {
        ExitStatus::from_raw(0)
    }

    fn fail_status() -> ExitStatus {
        ExitStatus::from_raw(1 << 8) // exit code 1
    }

    #[test]
    fn classify_ok_when_version_succeeds() {
        assert!(classify_version_check("pg_dump", Ok(ok_status())).is_ok());
    }

    #[test]
    fn classify_missing_tool_when_not_found() {
        let err = classify_version_check("pg_dump", Err(io::Error::from(io::ErrorKind::NotFound)))
            .unwrap_err();
        match err {
            MigrationError::MissingTool { tool, reason } => {
                assert_eq!(tool, "pg_dump");
                assert!(reason.contains("not found in $PATH"));
                // This runs before any server has been contacted, so the
                // reason must not name a PostgreSQL version it cannot know.
                assert!(
                    !reason.contains("postgresql-client-"),
                    "must not guess a client version: {reason}"
                );
            }
            other => panic!("expected MissingTool, got {other:?}"),
        }
    }

    #[test]
    fn name_required_major_uses_the_probed_version() {
        let err = MigrationError::missing_tool("pg_dump", "not found in $PATH");
        let msg = name_required_major(err, Some(16)).to_string();
        assert!(msg.contains("PostgreSQL 16"), "msg: {msg}");
        assert!(msg.contains("postgresql-client-16"), "msg: {msg}");
    }

    #[test]
    fn name_required_major_leaves_the_error_alone_when_the_probe_failed() {
        // Unreachable servers must not turn into an invented recommendation.
        let err = MigrationError::missing_tool("pg_restore", "not found in $PATH");
        let msg = name_required_major(err, None).to_string();
        assert!(msg.contains("not found in $PATH"), "msg: {msg}");
        assert!(
            !msg.contains("postgresql-client-1"),
            "must not name a version without a probe: {msg}"
        );
    }

    #[test]
    fn name_required_major_ignores_other_error_variants() {
        let err = MigrationError::config("something else");
        let msg = name_required_major(err, Some(18)).to_string();
        assert!(msg.contains("something else"), "msg: {msg}");
        assert!(!msg.contains("PostgreSQL 18"), "msg: {msg}");
    }

    #[test]
    fn classify_missing_tool_when_version_exits_nonzero() {
        let err = classify_version_check("pg_restore", Ok(fail_status())).unwrap_err();
        match err {
            MigrationError::MissingTool { tool, reason } => {
                assert_eq!(tool, "pg_restore");
                assert!(reason.contains("--version"));
            }
            other => panic!("expected MissingTool, got {other:?}"),
        }
    }

    #[test]
    fn classify_missing_tool_for_other_io_errors() {
        let err = classify_version_check(
            "pg_dump",
            Err(io::Error::from(io::ErrorKind::PermissionDenied)),
        )
        .unwrap_err();
        match err {
            MigrationError::MissingTool { tool, reason } => {
                assert_eq!(tool, "pg_dump");
                assert!(reason.contains("failed to spawn"));
            }
            other => panic!("expected MissingTool, got {other:?}"),
        }
    }

    #[test]
    fn missing_tool_error_message_includes_install_hint() {
        let err = MigrationError::missing_tool("pg_dump", "not found in $PATH");
        let msg = err.to_string();
        assert!(msg.contains("pg_dump"));
        assert!(msg.contains("not installed or not on $PATH"));
        assert!(msg.contains("postgresql-client"));
    }

    #[test]
    fn required_tools_includes_pg_dump_and_pg_restore() {
        assert!(REQUIRED_TOOLS.contains(&"pg_dump"));
        assert!(REQUIRED_TOOLS.contains(&"pg_restore"));
    }

    #[tokio::test]
    async fn verify_pg_tools_passes_in_test_env() {
        // Sanity test for the live path. CI hosts typically have these
        // tools; if they don't, the dump/restore tests would already be
        // useless. We tolerate either result so this test never blocks.
        let _ =
            verify_pg_tools_installed("postgres:///nonexistent", "postgres:///nonexistent").await;
    }

    #[test]
    fn maintenance_conn_swaps_database_name() {
        assert_eq!(
            maintenance_connection_string("postgresql://u:p@host:5432/mydb?sslmode=require"),
            "postgresql://u:p@host:5432/postgres?sslmode=require"
        );
    }

    #[test]
    fn maintenance_conn_no_query_params() {
        assert_eq!(
            maintenance_connection_string("postgresql://u:p@host:5432/mydb"),
            "postgresql://u:p@host:5432/postgres"
        );
    }

    #[test]
    fn maintenance_conn_preserves_multiple_query_params() {
        assert_eq!(
            maintenance_connection_string(
                "postgresql://u:p@host/db1?sslmode=require&connect_timeout=10"
            ),
            "postgresql://u:p@host/postgres?sslmode=require&connect_timeout=10"
        );
    }

    #[test]
    fn maintenance_conn_handles_no_password() {
        assert_eq!(
            maintenance_connection_string("postgresql://u@host/db1?sslmode=require"),
            "postgresql://u@host/postgres?sslmode=require"
        );
    }

    #[test]
    fn maintenance_conn_no_slash_after_host_returns_unchanged() {
        assert_eq!(
            maintenance_connection_string("postgresql://u:p@host:5432"),
            "postgresql://u:p@host:5432"
        );
    }

    #[test]
    fn maintenance_conn_no_slash_after_host_with_query_returns_unchanged() {
        assert_eq!(
            maintenance_connection_string("postgresql://u:p@host:5432?sslmode=require"),
            "postgresql://u:p@host:5432?sslmode=require"
        );
    }

    #[test]
    fn maintenance_conn_no_auth() {
        assert_eq!(
            maintenance_connection_string("postgresql://host:5432/mydb"),
            "postgresql://host:5432/postgres"
        );
    }

    #[test]
    fn maintenance_conn_password_with_at_sign() {
        assert_eq!(
            maintenance_connection_string("postgresql://u:p%40ss@host/db?sslmode=require"),
            "postgresql://u:p%40ss@host/postgres?sslmode=require"
        );
    }

    #[test]
    fn maintenance_conn_port_only_no_database() {
        assert_eq!(
            maintenance_connection_string("postgresql://u:p@host:5432"),
            "postgresql://u:p@host:5432"
        );
    }

    #[test]
    fn maintenance_conn_empty_database() {
        assert_eq!(
            maintenance_connection_string("postgresql://u:p@host:5432/"),
            "postgresql://u:p@host:5432/postgres"
        );
    }

    #[test]
    fn maintenance_conn_empty_database_with_query() {
        assert_eq!(
            maintenance_connection_string("postgresql://u:p@host:5432/?sslmode=require"),
            "postgresql://u:p@host:5432/postgres?sslmode=require"
        );
    }

    #[test]
    fn classify_version_check_ok_success_returns_ok() {
        let result = classify_version_check("tool_x", Ok(ok_status()));
        assert!(result.is_ok());
    }

    #[test]
    fn required_tools_length() {
        assert_eq!(REQUIRED_TOOLS.len(), 2);
    }

    #[test]
    fn build_publication_sql_all_tables() {
        let sql = build_create_publication_sql("my_pub", &[], &[]).unwrap();
        assert_eq!(sql, "CREATE PUBLICATION \"my_pub\" FOR ALL TABLES");
    }

    #[test]
    fn build_publication_sql_specific_tables() {
        let tables = vec!["public.users".to_string(), "public.orders".to_string()];
        let sql = build_create_publication_sql("my_pub", &tables, &[]).unwrap();
        assert_eq!(
            sql,
            "CREATE PUBLICATION \"my_pub\" FOR TABLE \"public\".\"users\", \"public\".\"orders\""
        );
    }

    #[test]
    fn build_publication_sql_specific_schemas() {
        let schemas = vec!["public".to_string(), "app".to_string()];
        let sql = build_create_publication_sql("my_pub", &[], &schemas).unwrap();
        assert_eq!(
            sql,
            "CREATE PUBLICATION \"my_pub\" FOR TABLES IN SCHEMA \"public\", \"app\""
        );
    }

    #[test]
    fn build_publication_sql_combines_tables_and_schemas() {
        let tables = vec!["public.users".to_string()];
        let schemas = vec!["app".to_string()];
        let sql = build_create_publication_sql("my_pub", &tables, &schemas).unwrap();
        assert_eq!(
            sql,
            "CREATE PUBLICATION \"my_pub\" FOR TABLE \"public\".\"users\", TABLES IN SCHEMA \"app\""
        );
    }

    #[test]
    fn build_publication_sql_quotes_special_chars() {
        let sql = build_create_publication_sql("pub\"name", &[], &[]).unwrap();
        assert!(sql.contains("\"pub\"\"name\""));
    }

    #[test]
    fn quote_qualified_name_unqualified() {
        let result = quote_qualified_name("users").unwrap();
        assert_eq!(result, "\"users\"");
    }

    #[test]
    fn quote_qualified_name_schema_qualified() {
        let result = quote_qualified_name("public.users").unwrap();
        assert_eq!(result, "\"public\".\"users\"");
    }

    #[test]
    fn quote_qualified_name_special_chars() {
        let result = quote_qualified_name("my schema.my table").unwrap();
        assert_eq!(result, "\"my schema\".\"my table\"");
    }

    #[test]
    fn quote_qualified_name_dot_in_table_part() {
        // Only splits on first dot: "schema.table.extra" -> "schema" + "table.extra"
        let result = quote_qualified_name("public.my.table").unwrap();
        assert_eq!(result, "\"public\".\"my.table\"");
    }

    #[test]
    fn quote_qualified_name_rejects_trailing_dot() {
        let result = quote_qualified_name("public.");
        assert!(result.is_err());
    }

    #[test]
    fn quote_qualified_name_rejects_leading_dot() {
        let result = quote_qualified_name(".table");
        assert!(result.is_err());
    }

    #[test]
    fn filter_tables_excludes_by_table_name() {
        let tables = vec![
            "public.users".into(),
            "public.orders".into(),
            "public.large_logs".into(),
        ];
        let result = filter_tables_by_exclusions(&tables, &["public.large_logs".into()], &[]);
        assert_eq!(result, vec!["public.users", "public.orders"]);
    }

    #[test]
    fn filter_tables_excludes_by_schema() {
        let tables = vec![
            "public.users".into(),
            "audit.events".into(),
            "audit.actions".into(),
            "app.config".into(),
        ];
        let result = filter_tables_by_exclusions(&tables, &[], &["audit".into()]);
        assert_eq!(result, vec!["public.users", "app.config"]);
    }

    #[test]
    fn filter_tables_excludes_both_table_and_schema() {
        let tables = vec![
            "public.users".into(),
            "public.large_logs".into(),
            "audit.events".into(),
            "app.config".into(),
        ];
        let result =
            filter_tables_by_exclusions(&tables, &["public.large_logs".into()], &["audit".into()]);
        assert_eq!(result, vec!["public.users", "app.config"]);
    }

    #[test]
    fn filter_tables_no_exclusions_returns_all() {
        let tables = vec!["public.users".into(), "public.orders".into()];
        let result = filter_tables_by_exclusions(&tables, &[], &[]);
        assert_eq!(result, tables);
    }

    #[test]
    fn filter_tables_empty_input() {
        let result: Vec<String> =
            filter_tables_by_exclusions(&[], &["public.x".into()], &["audit".into()]);
        assert!(result.is_empty());
    }

    #[test]
    fn filter_tables_exclude_all_matches_returns_empty() {
        let tables = vec!["audit.x".into(), "audit.y".into()];
        let result = filter_tables_by_exclusions(&tables, &[], &["audit".into()]);
        assert!(result.is_empty());
    }

    #[test]
    fn filter_tables_exclude_nonexistent_is_noop() {
        let tables = vec!["public.users".into()];
        let result = filter_tables_by_exclusions(
            &tables,
            &["public.nonexistent".into()],
            &["no_such_schema".into()],
        );
        assert_eq!(result, vec!["public.users"]);
    }

    #[test]
    fn filter_then_build_sql_excludes_correctly() {
        let all_tables: Vec<String> = vec![
            "public.users".into(),
            "public.orders".into(),
            "audit.logs".into(),
            "temp.scratch".into(),
        ];
        let filtered =
            filter_tables_by_exclusions(&all_tables, &["public.orders".into()], &["audit".into()]);
        let sql = build_create_publication_sql("my_pub", &filtered, &[]).unwrap();
        assert_eq!(
            sql,
            "CREATE PUBLICATION \"my_pub\" FOR TABLE \"public\".\"users\", \"temp\".\"scratch\""
        );
        assert!(!sql.contains("orders"));
        assert!(!sql.contains("audit"));
    }

    #[test]
    fn filter_schemas_from_include_list() {
        let schemas: Vec<String> = ["public", "audit", "app"]
            .iter()
            .map(|s| (*s).into())
            .collect();
        let exclude_schemas: Vec<String> = ["audit"].iter().map(|s| (*s).into()).collect();
        let filtered: Vec<String> = schemas
            .iter()
            .filter(|s| !exclude_schemas.iter().any(|ex| ex == *s))
            .cloned()
            .collect();
        assert_eq!(filtered, vec!["public", "app"]);
        let sql = build_create_publication_sql("p", &[], &filtered).unwrap();
        assert!(sql.contains("\"public\""));
        assert!(sql.contains("\"app\""));
        assert!(!sql.contains("\"audit\""));
    }

    #[test]
    fn preflight_report_summary_counts_pass_and_skip() {
        let mut r = PreflightReport::new();
        r.record("pg_tools", PreflightOutcome::Pass);
        r.record("version_compat", PreflightOutcome::Pass);
        r.record(
            "source_repl_role",
            PreflightOutcome::Skip {
                reason: "offline mode",
            },
        );
        assert_eq!(
            r.summary_line(),
            "preflight: 2 pass, 1 skip (pg_tools, version_compat, source_repl_role[skip])"
        );
    }

    #[test]
    fn preflight_report_summary_all_pass() {
        let mut r = PreflightReport::new();
        r.record("a", PreflightOutcome::Pass);
        r.record("b", PreflightOutcome::Pass);
        assert_eq!(r.summary_line(), "preflight: 2 pass (a, b)");
    }

    #[test]
    fn preflight_report_summary_empty() {
        let r = PreflightReport::new();
        assert_eq!(r.summary_line(), "preflight: 0 pass");
    }

    #[test]
    fn parse_pg_dump_version_plain() {
        assert_eq!(
            parse_pg_dump_version("pg_dump (PostgreSQL) 16.4\n"),
            Some(16),
        );
    }

    #[test]
    fn parse_pg_dump_version_ubuntu_suffix() {
        assert_eq!(
            parse_pg_dump_version("pg_dump (PostgreSQL) 15.7 (Ubuntu 15.7-1.pgdg22.04+1)\n",),
            Some(15),
        );
    }

    #[test]
    fn parse_pg_dump_version_release_candidate() {
        assert_eq!(
            parse_pg_dump_version("pg_dump (PostgreSQL) 17rc1\n"),
            Some(17),
        );
    }

    #[test]
    fn parse_pg_dump_version_pg9_two_part_major() {
        // Pre-PG10 used 9.6.24 etc; major is still "9" for our purposes.
        assert_eq!(
            parse_pg_dump_version("pg_dump (PostgreSQL) 9.6.24\n"),
            Some(9),
        );
    }

    #[test]
    fn parse_pg_dump_version_garbage_returns_none() {
        assert_eq!(parse_pg_dump_version(""), None);
        assert_eq!(parse_pg_dump_version("not a version"), None);
        assert_eq!(parse_pg_dump_version("pg_dump (PostgreSQL)"), None);
    }

    #[test]
    fn decide_pg_dump_compat_newer_or_equal_passes() {
        assert!(decide_pg_dump_compat(17, 15).is_ok());
        assert!(decide_pg_dump_compat(15, 15).is_ok());
    }

    #[test]
    fn decide_pg_dump_compat_older_than_source_fails() {
        let err = decide_pg_dump_compat(14, 16).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("pg_dump is version 14"), "msg: {msg}");
        assert!(msg.contains("source server is 16"), "msg: {msg}");
        assert!(msg.contains("postgresql-client-16"), "msg: {msg}");
    }

    #[test]
    fn decide_pg_dump_compat_does_not_consider_target() {
        // pg_dump 14 against source 14, target 16 — pg_dump is fine; the
        // target's higher major only constrains pg_restore.
        assert!(decide_pg_dump_compat(14, 14).is_ok());
    }

    #[test]
    fn decide_pg_restore_compat_newer_or_equal_passes() {
        // Same major on both sides is always fine.
        assert!(decide_pg_restore_compat(16, 16, 16).is_ok());
        // pg_restore 17 vs target 16, dump produced by pg_dump 15 — fine.
        assert!(decide_pg_restore_compat(17, 16, 15).is_ok());
        // A newer client than the target is allowed: it costs an ignored
        // `SET transaction_timeout` error on pre-17 targets, not lost data.
        // install.sh picks the right major up front so this is rare.
        assert!(decide_pg_restore_compat(18, 16, 16).is_ok());
        assert!(decide_pg_restore_compat(18, 17, 16).is_ok());
        assert!(decide_pg_restore_compat(17, 17, 15).is_ok());
    }

    #[test]
    fn decide_pg_restore_compat_older_than_target_fails() {
        let err = decide_pg_restore_compat(14, 16, 14).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("pg_restore is version 14"), "msg: {msg}");
        assert!(msg.contains("target server is 16"), "msg: {msg}");
    }

    #[test]
    fn decide_pg_restore_compat_older_than_pg_dump_fails() {
        // pg_restore 15 against target 14 satisfies the target check, but
        // the dump was produced by pg_dump 16 — pg_restore can't read it.
        let err = decide_pg_restore_compat(15, 14, 16).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("pg_restore is version 15"), "msg: {msg}");
        assert!(msg.contains("pg_dump is version 16"), "msg: {msg}");
    }

    #[test]
    fn pg_dump_and_pg_restore_compat_allow_cross_version_upgrade() {
        // The motivating case: PG14 -> PG16 with pg_dump 14 + pg_restore 16.
        // Both helpers must pass.
        assert!(decide_pg_dump_compat(14, 14).is_ok());
        assert!(decide_pg_restore_compat(16, 16, 14).is_ok());
    }

    #[test]
    fn target_role_privilege_decision_offline_only_needs_create_db_priv() {
        // Offline never touches subscriptions; only CREATE on database matters.
        assert!(target_role_privilege_decision(
            MigrationMode::Offline,
            /*server_major*/ 14,
            /*has_create_db*/ true,
            /*is_super*/ false,
            /*has_create_sub_role*/ false,
        )
        .is_ok());
    }

    #[test]
    fn target_role_privilege_decision_offline_fails_without_create() {
        let err = target_role_privilege_decision(MigrationMode::Offline, 14, false, false, false)
            .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("CREATE"), "msg: {msg}");
    }

    #[test]
    fn target_role_privilege_decision_online_pg16_passes_with_pg_create_subscription_member() {
        assert!(
            target_role_privilege_decision(MigrationMode::Online, 16, true, false, true).is_ok()
        );
    }

    #[test]
    fn target_role_privilege_decision_online_pg16_passes_with_superuser() {
        assert!(
            target_role_privilege_decision(MigrationMode::Online, 16, true, true, false).is_ok()
        );
    }

    #[test]
    fn target_role_privilege_decision_online_pg16_fails_without_either() {
        let err = target_role_privilege_decision(MigrationMode::Online, 16, true, false, false)
            .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("pg_create_subscription"), "msg: {msg}");
    }

    #[test]
    fn target_role_privilege_decision_online_pg14_requires_superuser() {
        let err = target_role_privilege_decision(MigrationMode::Online, 14, true, false, false)
            .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("superuser"), "msg: {msg}");
        assert!(
            target_role_privilege_decision(MigrationMode::Online, 14, true, true, false).is_ok()
        );
    }

    #[test]
    fn classify_subscription_capacity_fails_when_lrw_zero() {
        let err = classify_subscription_capacity(0, 8, 2).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("max_logical_replication_workers"),
            "msg: {msg}"
        );
    }

    #[test]
    fn classify_subscription_capacity_fails_when_mwp_zero() {
        let err = classify_subscription_capacity(4, 0, 2).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("max_worker_processes"), "msg: {msg}");
    }

    #[test]
    fn classify_subscription_capacity_passes_at_recommended_minimum() {
        // RECOMMENDED_LRW = 1, RECOMMENDED_MWP = 2 — the absolute floor
        // required for CREATE SUBSCRIPTION to function at all.
        assert!(classify_subscription_capacity(1, 2, 1).is_ok());
    }

    #[test]
    fn classify_subscription_capacity_passes_at_production_floor() {
        // Production-recommended values (4 LRW / 8 MWP) — still well above
        // the new absolute-minimum thresholds.
        assert!(classify_subscription_capacity(4, 8, 2).is_ok());
    }

    #[test]
    fn classify_subscription_capacity_passes_above_recommended() {
        assert!(classify_subscription_capacity(16, 32, 4).is_ok());
    }

    #[test]
    fn offline_preflight_includes_expected_checks() {
        let names = offline_preflight_check_names();
        assert_eq!(
            names,
            &[
                "pg_tools",
                "version_compat",
                "target_not_in_recovery",
                "target_db_exists",
                "target_role_privs",
                "extensions_available",
            ]
        );
    }

    #[test]
    fn online_preflight_includes_expected_checks() {
        let names = online_preflight_check_names();
        assert_eq!(
            names,
            &[
                "pg_tools",
                "version_compat",
                "source_repl_role",
                "source_snapshot_timeouts",
                "target_not_in_recovery",
                "target_db_exists",
                "target_role_privs",
                "extensions_available",
                "source_logical_repl",
                "target_sub_capacity",
                "pglogical_clean",
            ]
        );
    }

    #[test]
    fn preflight_report_names_returns_recorded_in_order() {
        let mut r = PreflightReport::new();
        r.record("a", PreflightOutcome::Pass);
        r.record("b", PreflightOutcome::Skip { reason: "x" });
        r.record("c", PreflightOutcome::Pass);
        assert_eq!(r.names(), vec!["a", "b", "c"]);
    }

    #[test]
    fn preflight_outcome_skip_equality() {
        let a = PreflightOutcome::Skip { reason: "r" };
        let b = PreflightOutcome::Skip { reason: "r" };
        assert_eq!(a, b);
        assert_ne!(a, PreflightOutcome::Pass);
    }

    #[test]
    fn parse_pg_dump_version_enterprisedb_no_paren() {
        // EnterpriseDB / Postgres Pro builds may print without the
        // `(PostgreSQL)` marker. The fallback path scans for the first
        // digit-led whitespace token.
        assert_eq!(parse_pg_dump_version("edb_dump 15.4\n"), Some(15));
        assert_eq!(parse_pg_dump_version("custom-tool 17 ee\n"), Some(17));
    }

    #[tokio::test]
    async fn capture_tool_version_returns_version_for_pg_dump() {
        // pg_dump is in PATH on any environment that can build/test pg_dbmigrator
        // (the project lists it in REQUIRED_TOOLS and integration CI installs it).
        let out = capture_tool_version("pg_dump")
            .await
            .expect("pg_dump exists");
        assert!(out.contains("pg_dump"), "stdout: {out}");
    }

    #[tokio::test]
    async fn capture_tool_version_errors_when_binary_missing() {
        let err = capture_tool_version("pg_dbmigrator_definitely_not_a_real_binary_xyz_42")
            .await
            .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("failed to spawn") || msg.contains("not found"),
            "msg: {msg}"
        );
    }

    // ----- Mock-probe driven tests for the async verify_*_with_probe wrappers.
    // These exercise the orchestration around each pure decision helper
    // without needing a live PostgreSQL or pg_dump binary.

    use std::sync::Mutex;

    /// Test double for [`PreflightProbe`]. Each field is the value the
    /// corresponding trait method returns; unset fields cause the method
    /// to panic — keep tests honest about which probe calls they exercise.
    #[derive(Default)]
    struct MockProbe {
        tool_versions: Mutex<std::collections::HashMap<String, Result<String>>>,
        source_major: Option<u32>,
        target_major: Option<u32>,
        role_has_repl_or_super: Option<bool>,
        target_role_info: Option<TargetRolePrivInfo>,
        in_recovery: Option<bool>,
        sub_capacity: Option<(i64, i64, i64)>,
        extensions: Option<(Vec<String>, Vec<String>)>,
        idle_timeout_ms: Option<i64>,
    }

    impl MockProbe {
        fn with_tool(self, tool: &str, out: &str) -> Self {
            self.tool_versions
                .lock()
                .unwrap()
                .insert(tool.to_string(), Ok(out.to_string()));
            self
        }
    }

    #[async_trait]
    impl PreflightProbe for MockProbe {
        async fn capture_tool_version(&self, tool: &str) -> Result<String> {
            let mut map = self.tool_versions.lock().unwrap();
            match map.remove(tool) {
                Some(r) => r,
                None => panic!("MockProbe.capture_tool_version({tool}) not stubbed"),
            }
        }
        async fn server_major_version(&self, conn: &str) -> Result<u32> {
            // Distinguish source vs target by substring in the conn string.
            if conn.contains("source") {
                Ok(self.source_major.expect("source_major not stubbed"))
            } else {
                Ok(self.target_major.expect("target_major not stubbed"))
            }
        }
        async fn role_has_replication_or_super(&self, _conn: &str) -> Result<bool> {
            Ok(self
                .role_has_repl_or_super
                .expect("role_has_repl_or_super not stubbed"))
        }
        async fn target_role_privilege_info(&self, _conn: &str) -> Result<TargetRolePrivInfo> {
            Ok(self.target_role_info.expect("target_role_info not stubbed"))
        }
        async fn is_in_recovery(&self, _conn: &str) -> Result<bool> {
            Ok(self.in_recovery.expect("in_recovery not stubbed"))
        }
        async fn subscription_capacity_gucs(&self, _conn: &str) -> Result<(i64, i64, i64)> {
            Ok(self.sub_capacity.expect("sub_capacity not stubbed"))
        }
        async fn idle_in_transaction_timeout_ms(&self, _conn: &str) -> Result<i64> {
            Ok(self.idle_timeout_ms.expect("idle_timeout_ms not stubbed"))
        }
        async fn installed_and_available_extensions(
            &self,
            _source_conn: &str,
            _target_conn: &str,
        ) -> Result<(Vec<String>, Vec<String>)> {
            Ok(self.extensions.clone().expect("extensions not stubbed"))
        }
    }

    #[tokio::test]
    async fn verify_pg_dump_version_compat_with_probe_passes_when_binary_dominates() {
        let probe = MockProbe {
            source_major: Some(16),
            target_major: Some(17),
            ..Default::default()
        }
        .with_tool("pg_dump", "pg_dump (PostgreSQL) 17.0\n")
        .with_tool("pg_restore", "pg_restore (PostgreSQL) 17.0\n");
        verify_pg_dump_version_compat_with_probe(&probe, "src://source", "tgt://target")
            .await
            .expect("should pass");
    }

    #[tokio::test]
    async fn verify_pg_dump_version_compat_with_probe_fails_when_binary_too_old() {
        let probe = MockProbe {
            source_major: Some(17),
            target_major: Some(17),
            ..Default::default()
        }
        .with_tool("pg_dump", "pg_dump (PostgreSQL) 15.0\n")
        .with_tool("pg_restore", "pg_restore (PostgreSQL) 15.0\n");
        let err = verify_pg_dump_version_compat_with_probe(&probe, "src://source", "tgt://target")
            .await
            .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("postgresql-client-17"), "msg: {msg}");
    }

    #[tokio::test]
    async fn verify_pg_dump_version_compat_with_probe_rejects_unparseable_pg_dump() {
        let probe = MockProbe {
            source_major: Some(17),
            target_major: Some(17),
            ..Default::default()
        }
        .with_tool("pg_dump", "garbage output\n")
        .with_tool("pg_restore", "pg_restore (PostgreSQL) 17.0\n");
        let err = verify_pg_dump_version_compat_with_probe(&probe, "src://source", "tgt://target")
            .await
            .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("could not parse pg_dump version"),
            "msg: {msg}"
        );
    }

    #[tokio::test]
    async fn verify_pg_dump_version_compat_with_probe_rejects_unparseable_pg_restore() {
        let probe = MockProbe {
            source_major: Some(17),
            target_major: Some(17),
            ..Default::default()
        }
        .with_tool("pg_dump", "pg_dump (PostgreSQL) 17.0\n")
        .with_tool("pg_restore", "not a version string");
        let err = verify_pg_dump_version_compat_with_probe(&probe, "src://source", "tgt://target")
            .await
            .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("could not parse pg_restore version"),
            "msg: {msg}"
        );
    }

    #[tokio::test]
    async fn recommended_client_major_takes_the_newer_server() {
        // Upgrade direction: a PG14 source into a PG16 target needs a 16
        // client -- pg_restore must be at least as new as the target.
        let probe = MockProbe {
            source_major: Some(14),
            target_major: Some(16),
            ..Default::default()
        };
        let major = recommended_client_major_with_probe(&probe, "src://source", "tgt://target")
            .await
            .expect("should probe");
        assert_eq!(major, 16);
    }

    #[tokio::test]
    async fn recommended_client_major_takes_the_source_when_it_is_newer() {
        // Downgrade direction: pg_dump still has to read a PG18 source, so the
        // client cannot drop to the target's 15.
        let probe = MockProbe {
            source_major: Some(18),
            target_major: Some(15),
            ..Default::default()
        };
        let major = recommended_client_major_with_probe(&probe, "src://source", "tgt://target")
            .await
            .expect("should probe");
        assert_eq!(major, 18);
    }

    #[tokio::test]
    async fn recommended_client_major_when_both_servers_match() {
        let probe = MockProbe {
            source_major: Some(17),
            target_major: Some(17),
            ..Default::default()
        };
        let major = recommended_client_major_with_probe(&probe, "src://source", "tgt://target")
            .await
            .expect("should probe");
        assert_eq!(major, 17);
    }

    #[tokio::test]
    async fn recommended_client_major_never_below_either_server() {
        // Whatever the pair, the answer must satisfy both compatibility rules
        // that decide_pg_dump_compat / decide_pg_restore_compat enforce later.
        for (src, tgt) in [(13u32, 18u32), (18, 13), (16, 16), (9, 17)] {
            let probe = MockProbe {
                source_major: Some(src),
                target_major: Some(tgt),
                ..Default::default()
            };
            let major = recommended_client_major_with_probe(&probe, "src://source", "tgt://target")
                .await
                .expect("should probe");
            assert!(major >= src && major >= tgt, "{major} for ({src}, {tgt})");
            decide_pg_dump_compat(major, src).expect("pg_dump must accept the source");
            decide_pg_restore_compat(major, tgt, major).expect("pg_restore must accept the target");
        }
    }

    #[tokio::test]
    async fn verify_source_replication_role_with_probe_passes_when_has_repl_or_super() {
        let probe = MockProbe {
            role_has_repl_or_super: Some(true),
            ..Default::default()
        };
        verify_source_replication_role_with_probe(&probe, "src://source")
            .await
            .expect("should pass");
    }

    #[tokio::test]
    async fn verify_source_replication_role_with_probe_fails_when_role_cannot_replicate() {
        let probe = MockProbe {
            role_has_repl_or_super: Some(false),
            ..Default::default()
        };
        let err = verify_source_replication_role_with_probe(&probe, "src://source")
            .await
            .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("REPLICATION attribute"), "msg: {msg}");
        assert!(msg.contains("ALTER ROLE"), "msg: {msg}");
    }

    #[tokio::test]
    async fn verify_target_not_in_recovery_with_probe_passes_for_primary() {
        let probe = MockProbe {
            in_recovery: Some(false),
            ..Default::default()
        };
        verify_target_not_in_recovery_with_probe(&probe, "tgt://target")
            .await
            .expect("should pass");
    }

    #[tokio::test]
    async fn verify_target_not_in_recovery_with_probe_fails_for_standby() {
        let probe = MockProbe {
            in_recovery: Some(true),
            ..Default::default()
        };
        let err = verify_target_not_in_recovery_with_probe(&probe, "tgt://target")
            .await
            .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("hot standby"), "msg: {msg}");
        assert!(msg.contains("pg_promote"), "msg: {msg}");
    }

    #[tokio::test]
    async fn verify_target_subscription_capacity_with_probe_passes_above_recommended() {
        let probe = MockProbe {
            sub_capacity: Some((8, 16, 4)),
            ..Default::default()
        };
        verify_target_subscription_capacity_with_probe(&probe, "tgt://target")
            .await
            .expect("should pass");
    }

    #[tokio::test]
    async fn verify_target_subscription_capacity_with_probe_fails_when_lrw_zero() {
        let probe = MockProbe {
            sub_capacity: Some((0, 16, 4)),
            ..Default::default()
        };
        let err = verify_target_subscription_capacity_with_probe(&probe, "tgt://target")
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("max_logical_replication_workers"));
    }

    #[tokio::test]
    async fn verify_target_subscription_capacity_with_probe_passes_at_floor() {
        // (1, 2, 1) — exactly the absolute floor required by the trait.
        let probe = MockProbe {
            sub_capacity: Some((1, 2, 1)),
            ..Default::default()
        };
        verify_target_subscription_capacity_with_probe(&probe, "tgt://target")
            .await
            .expect("should pass at the floor");
    }

    #[tokio::test]
    async fn verify_target_role_privileges_with_probe_passes_for_pg16_member() {
        let probe = MockProbe {
            target_role_info: Some(TargetRolePrivInfo {
                target_major: 16,
                has_create: true,
                is_super: false,
                has_sub_role: true,
            }),
            ..Default::default()
        };
        verify_target_role_privileges_with_probe(&probe, "tgt://target", MigrationMode::Online)
            .await
            .expect("should pass");
    }

    #[tokio::test]
    async fn verify_target_role_privileges_with_probe_passes_offline_with_create_only() {
        let probe = MockProbe {
            target_role_info: Some(TargetRolePrivInfo {
                target_major: 14,
                has_create: true,
                is_super: false,
                has_sub_role: false,
            }),
            ..Default::default()
        };
        verify_target_role_privileges_with_probe(&probe, "tgt://target", MigrationMode::Offline)
            .await
            .expect("should pass");
    }

    #[tokio::test]
    async fn verify_target_role_privileges_with_probe_fails_pg15_non_super_online() {
        let probe = MockProbe {
            target_role_info: Some(TargetRolePrivInfo {
                target_major: 15,
                has_create: true,
                is_super: false,
                has_sub_role: false,
            }),
            ..Default::default()
        };
        let err =
            verify_target_role_privileges_with_probe(&probe, "tgt://target", MigrationMode::Online)
                .await
                .unwrap_err();
        assert!(format!("{err}").contains("superuser"));
    }

    #[tokio::test]
    async fn verify_target_role_privileges_with_probe_fails_pg16_non_member_non_super_online() {
        let probe = MockProbe {
            target_role_info: Some(TargetRolePrivInfo {
                target_major: 16,
                has_create: true,
                is_super: false,
                has_sub_role: false,
            }),
            ..Default::default()
        };
        let err =
            verify_target_role_privileges_with_probe(&probe, "tgt://target", MigrationMode::Online)
                .await
                .unwrap_err();
        assert!(format!("{err}").contains("pg_create_subscription"));
    }

    #[tokio::test]
    async fn verify_target_role_privileges_with_probe_fails_offline_without_create() {
        let probe = MockProbe {
            target_role_info: Some(TargetRolePrivInfo {
                target_major: 16,
                has_create: false,
                is_super: false,
                has_sub_role: false,
            }),
            ..Default::default()
        };
        let err = verify_target_role_privileges_with_probe(
            &probe,
            "tgt://target",
            MigrationMode::Offline,
        )
        .await
        .unwrap_err();
        assert!(format!("{err}").contains("CREATE"));
    }

    #[tokio::test]
    async fn verify_target_extensions_available_with_probe_passes_when_all_available() {
        let probe = MockProbe {
            extensions: Some((
                vec!["plpgsql".to_string(), "hstore".to_string()],
                vec![
                    "plpgsql".to_string(),
                    "hstore".to_string(),
                    "postgis".to_string(),
                ],
            )),
            ..Default::default()
        };
        verify_target_extensions_available_with_probe(&probe, "src://source", "tgt://target")
            .await
            .expect("all source extensions available on target");
    }

    #[tokio::test]
    async fn verify_target_extensions_available_with_probe_fails_when_missing() {
        let probe = MockProbe {
            extensions: Some((
                vec!["plpgsql".to_string(), "timescaledb".to_string()],
                vec!["plpgsql".to_string()],
            )),
            ..Default::default()
        };
        let err =
            verify_target_extensions_available_with_probe(&probe, "src://source", "tgt://target")
                .await
                .unwrap_err();
        assert!(format!("{err}").contains("timescaledb"), "err: {err}");
    }

    #[test]
    fn classify_snapshot_timeout_accepts_disabled() {
        assert!(classify_snapshot_timeout(0).is_ok());
    }

    #[test]
    fn classify_snapshot_timeout_rejects_any_non_zero() {
        // Even a generous timeout is rejected: the snapshot connection is
        // idle for the whole dump, and we cannot know the dump's duration
        // up front.
        for ms in [1, 60_000, 3_600_000] {
            let err = classify_snapshot_timeout(ms).unwrap_err();
            let msg = format!("{err}");
            assert!(
                msg.contains(&ms.to_string()),
                "err should name the value: {msg}"
            );
            assert!(
                msg.contains("ALTER ROLE"),
                "err should be actionable: {msg}"
            );
        }
    }

    #[tokio::test]
    async fn verify_source_snapshot_timeouts_passes_when_disabled() {
        let probe = MockProbe {
            idle_timeout_ms: Some(0),
            ..Default::default()
        };
        assert!(
            verify_source_snapshot_timeouts_with_probe(&probe, "src://source")
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn verify_source_snapshot_timeouts_fails_when_set() {
        let probe = MockProbe {
            idle_timeout_ms: Some(600_000),
            ..Default::default()
        };
        let err = verify_source_snapshot_timeouts_with_probe(&probe, "src://source")
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("600000"), "err: {err}");
    }

    #[test]
    fn online_bundle_includes_snapshot_timeout_check() {
        assert!(online_preflight_check_names().contains(&"source_snapshot_timeouts"));
        // Offline takes no snapshot, so the check must not appear there.
        assert!(!offline_preflight_check_names().contains(&"source_snapshot_timeouts"));
    }
}
