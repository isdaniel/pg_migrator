//! Wrapper around the `pg_dump` external command.
//!
//! The actual process is invoked through a [`CommandRunner`] trait so that
//! unit tests can substitute a deterministic implementation without requiring
//! a real PostgreSQL installation. The default [`TokioCommandRunner`] simply
//! shells out to `pg_dump` via [`tokio::process::Command`].

use std::path::{Path, PathBuf};
use std::process::Stdio;

use async_trait::async_trait;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::{DumpScope, EndpointConfig};
use crate::error::{MigrationError, Result};
use crate::restore::{ingest_pg_restore_stderr_line, RestoreErrorSummary};

/// Description of a `pg_dump` invocation.
#[derive(Debug, Clone)]
pub struct DumpRequest {
    /// Source endpoint to dump from.
    pub source: EndpointConfig,
    /// What to dump (schema, data, or both).
    pub scope: DumpScope,
    /// Number of parallel jobs (`--jobs`). Only honored for directory format
    /// dumps; ignored for the plain custom format.
    pub jobs: usize,
    /// Optional snapshot name to use (`--snapshot=<name>`). Required for
    /// online migrations to obtain a consistent dump aligned with the
    /// replication slot's start LSN.
    pub snapshot: Option<String>,
    /// Schemas to include (`--schema=...`).
    pub schemas: Vec<String>,
    /// Tables to include (`--table=...`).
    pub tables: Vec<String>,
    /// Schemas to **exclude** (`--exclude-schema=...`). Combined with
    /// `schemas` (which restricts the include set), this lets the
    /// operator both opt-in a tenant schema and opt-out an audit
    /// schema in the same dump invocation. `pg_dump` evaluates exclude
    /// rules after include rules, so an excluded child of an included
    /// schema is correctly omitted.
    pub exclude_schemas: Vec<String>,
    /// Tables to **exclude** (`--exclude-table=...`).
    pub exclude_tables: Vec<String>,
    /// Tables whose *definition* is dumped but whose rows are not
    /// (`--exclude-table-data=...`).
    ///
    /// Used by [`crate::copy_split`]: a table too large for `pg_dump`'s
    /// per-entry parallelism keeps its `CREATE TABLE` in the archive, so
    /// the pre-data restore still creates it, while its rows travel
    /// straight from source to target instead of through the archive.
    pub exclude_table_data: Vec<String>,
    /// Where to write the dump archive.
    pub output_path: PathBuf,
    /// Output format. Defaults to [`DumpFormat::Custom`].
    pub format: DumpFormat,
    /// Pass `--no-publications` to `pg_dump`. Default `true`. The source's
    /// publications are an implementation detail of *this* migration; if they
    /// land on the target they will be recreated and emit
    /// `wal_level is insufficient to publish logical changes` warnings on a
    /// target that doesn't run logical replication. Set to `false` only when
    /// the target legitimately needs to inherit the source's publications.
    pub no_publications: bool,
    /// Pass `--no-subscriptions` to `pg_dump`. Default `true`. Same rationale
    /// as `no_publications` — a fresh target should not inherit subscription
    /// definitions that point at the *previous* upstream.
    pub no_subscriptions: bool,
    /// Optional `--compress=<spec>` value to forward to `pg_dump` (PG 16+
    /// accepts `gzip:N`, `lz4:N`, `zstd:N`, or `none`; older versions accept
    /// integer levels `0..=9`). Compressing the archive trades CPU for
    /// network/disk: `lz4:1` and `zstd:3` are typically a 3–10× size win on
    /// schema-heavy data with negligible dump-time overhead. `None` means
    /// "do not pass `--compress` at all" — `pg_dump` then uses the format's
    /// default (gzip level 6 for `Custom` / `Directory`).
    pub compress: Option<String>,
    /// Pass `--no-sync` to `pg_dump`. Default `true`. The dump archive is a
    /// transient artefact consumed by `pg_restore` immediately afterwards —
    /// fsyncing every output file on close is pure I/O overhead.
    pub no_sync: bool,
    /// Pass `--no-comments` to `pg_dump`. Default `true`. Skips COMMENT ON
    /// statements that are rarely needed on the migration target.
    pub no_comments: bool,
    /// Pass `--no-security-labels` to `pg_dump`. Default `true`. Skips
    /// SE-Linux security labels that are typically irrelevant on the target.
    pub no_security_labels: bool,
    /// Pass `--no-table-access-method` to `pg_dump`. Default `false`.
    /// PG 15+ — omits `USING <am>` clauses from CREATE TABLE.
    pub no_table_access_method: bool,
}

/// `pg_dump` archive format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DumpFormat {
    /// Custom archive (`-F c`).
    Custom,
    /// Plain SQL (`-F p`).
    Plain,
    /// Directory archive (`-F d`) — required when using `--jobs > 1`.
    Directory,
}

impl DumpFormat {
    /// Returns the `-F` flag value used by `pg_dump`.
    pub fn flag(self) -> &'static str {
        match self {
            Self::Custom => "c",
            Self::Plain => "p",
            Self::Directory => "d",
        }
    }
}

/// Build the argv vector used to invoke `pg_dump` for a [`DumpRequest`].
///
/// This is split out from the actual process spawn so that tests can assert
/// on the produced command line without touching the filesystem.
pub fn build_pg_dump_args(req: &DumpRequest) -> Vec<String> {
    let mut args: Vec<String> = vec![
        "--no-password".into(),
        "--verbose".into(),
        "--format".into(),
        req.format.flag().into(),
        "--file".into(),
        req.output_path.to_string_lossy().into_owned(),
        "--host".into(),
        req.source.host.clone(),
        "--port".into(),
        req.source.port.to_string(),
        "--username".into(),
        req.source.user.clone(),
        "--dbname".into(),
        req.source.database.clone(),
    ];

    if let Some(flag) = req.scope.pg_dump_flag() {
        args.push(flag.into());
    }

    if req.format == DumpFormat::Directory && req.jobs > 1 {
        args.push("--jobs".into());
        args.push(req.jobs.to_string());
    }

    if let Some(snap) = &req.snapshot {
        args.push(format!("--snapshot={snap}"));
    }

    for s in &req.schemas {
        args.push(format!("--schema={s}"));
    }

    for t in &req.tables {
        args.push(format!("--table={t}"));
    }

    for s in &req.exclude_schemas {
        args.push(format!("--exclude-schema={s}"));
    }

    for t in &req.exclude_tables {
        args.push(format!("--exclude-table={t}"));
    }

    for t in &req.exclude_table_data {
        args.push(format!("--exclude-table-data={t}"));
    }

    if req.no_publications {
        args.push("--no-publications".into());
    }
    if req.no_subscriptions {
        args.push("--no-subscriptions".into());
    }
    if let Some(spec) = &req.compress {
        // PG 16+ accepts `gzip:N`, `lz4:N`, `zstd:N`, `none`; older
        // versions accept a bare digit `0..=9`. Pass through verbatim.
        args.push(format!("--compress={spec}"));
    }
    if req.no_sync {
        args.push("--no-sync".into());
    }
    if req.no_comments {
        args.push("--no-comments".into());
    }
    if req.no_security_labels {
        args.push("--no-security-labels".into());
    }
    if req.no_table_access_method {
        args.push("--no-table-access-method".into());
    }

    args
}

/// Trait abstracting an external command execution. The default
/// implementation is [`TokioCommandRunner`].
///
/// Implementations must honour `cancel`: if the token fires while the child
/// is running, the child should be killed and the call should return
/// [`MigrationError::Cancelled`]. Without this, a multi-hour `pg_dump`
/// would ignore Ctrl+C until completion.
#[async_trait]
pub trait CommandRunner: Send + Sync + std::fmt::Debug {
    /// Run `program` with `args` and the given environment additions. Should
    /// fail if the process exits with a non-zero status, or return
    /// [`MigrationError::Cancelled`] if `cancel` fires before the child
    /// exits.
    async fn run(
        &self,
        program: &str,
        args: &[String],
        env: &[(String, String)],
        cancel: &CancellationToken,
    ) -> Result<()>;
}

/// Default [`CommandRunner`] that uses [`tokio::process::Command`].
///
/// On Unix the child is launched in its own process group via
/// `setpgid(0, 0)`. `pg_dump --jobs N` forks worker processes that share
/// the leader's pgid, so on cancellation we deliver SIGTERM to the
/// **whole group** (`kill(-pgid, SIGTERM)`) and escalate to SIGKILL
/// after a short grace window. Without this, killing only the leader
/// leaves the workers re-parented to PID 1 and still pumping bytes from
/// the source. `kill_on_drop` is also set so a cancelled future never
/// leaks even if the explicit kill path takes the slow road.
#[derive(Debug, Default, Clone)]
pub struct TokioCommandRunner;

#[async_trait]
impl CommandRunner for TokioCommandRunner {
    async fn run(
        &self,
        program: &str,
        args: &[String],
        env: &[(String, String)],
        cancel: &CancellationToken,
    ) -> Result<()> {
        debug!(program, ?args, "spawning external command");

        // Capture stderr for `pg_restore` (error categorization) and
        // `pg_dump` (table-level progress). Both emit useful structured
        // lines on stderr when `--verbose` is active.
        let capture_stderr = program == "pg_restore" || program == "pg_dump";

        let mut cmd = tokio::process::Command::new(program);
        cmd.args(args);
        for (k, v) in env {
            cmd.env(k, v);
        }
        cmd.stdin(Stdio::null());
        cmd.stdout(Stdio::inherit());
        if capture_stderr {
            cmd.stderr(Stdio::piped());
        } else {
            cmd.stderr(Stdio::inherit());
        }
        cmd.kill_on_drop(true);
        #[cfg(unix)]
        cmd.process_group(0);

        let mut child = cmd
            .spawn()
            .map_err(|e| MigrationError::external(program, format!("failed to spawn: {e}")))?;
        let child_pid = child.id();

        // If we asked for `Stdio::piped()`, spawn a reader task that
        // drains stderr concurrently with `child.wait()`. The reader
        // (a) tees every line straight to our own stderr so the
        // operator still sees live progress, and (b) accumulates a
        // bounded structured summary of error/warning lines.
        let is_restore = program == "pg_restore";
        let stderr_task = if capture_stderr {
            child.stderr.take().map(|pipe| {
                tokio::spawn(async move {
                    let mut summary = RestoreErrorSummary::default();
                    let mut reader = BufReader::new(pipe).lines();
                    let mut sink = tokio::io::stderr();
                    loop {
                        match reader.next_line().await {
                            Ok(Some(line)) => {
                                // Tee live so existing operator UX is
                                // preserved.
                                let _ = sink.write_all(line.as_bytes()).await;
                                let _ = sink.write_all(b"\n").await;
                                let _ = sink.flush().await;
                                if is_restore {
                                    ingest_pg_restore_stderr_line(&line, &mut summary);
                                }
                            }
                            Ok(None) => break,
                            Err(_) => break,
                        }
                    }
                    summary
                })
            })
        } else {
            None
        };

        let status = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                warn!(program, "cancellation requested — terminating child group");
                kill_child_group(child_pid, /* sigkill = */ false);
                // Give pg_dump's worker pool ~2 s to flush & close socket
                // connections, then escalate to SIGKILL.
                let timeout = tokio::time::sleep(std::time::Duration::from_secs(2));
                tokio::pin!(timeout);
                tokio::select! {
                    res = child.wait() => { let _ = res; }
                    _ = &mut timeout => {
                        kill_child_group(child_pid, /* sigkill = */ true);
                        let _ = child.start_kill();
                        let _ = child.wait().await;
                    }
                }
                // Best-effort: drain the stderr task before we leave.
                if let Some(t) = stderr_task {
                    let _ = t.await;
                }
                return Err(MigrationError::Cancelled);
            }
            res = child.wait() => res.map_err(|e| {
                MigrationError::external(program, format!("wait failed: {e}"))
            })?,
        };

        // Wait for the stderr drainer to flush the rest of the buffer.
        let summary = if let Some(t) = stderr_task {
            t.await.ok()
        } else {
            None
        };

        if !status.success() {
            // Embed the categorized summary into the error message so
            // callers (and the warn log) get an actionable report
            // without any extra plumbing.
            let detail = match summary {
                Some(s) if !s.is_empty() || s.errors_ignored_reported.is_some() => {
                    format!(
                        "exited with status {status}\n\n{report}",
                        report = s.render_report()
                    )
                }
                _ => format!("exited with status {status}"),
            };
            return Err(MigrationError::external(program, detail));
        }

        info!(program, "external command finished successfully");
        Ok(())
    }
}

/// Send SIGTERM (or SIGKILL when `sigkill = true`) to the process group
/// led by `pid`. No-op on non-Unix platforms; `kill_on_drop(true)` plus
/// the parent's normal exit handle the cleanup there.
#[cfg(unix)]
fn kill_child_group(pid: Option<u32>, sigkill: bool) {
    if let Some(pid) = pid {
        let pgid = pid as libc::pid_t;
        let sig = if sigkill {
            libc::SIGKILL
        } else {
            libc::SIGTERM
        };
        // SAFETY: kill() is async-signal-safe and only takes integers;
        // the negative PID dispatches to the entire process group. A
        // failure (e.g. group already gone) is logged but not actionable.
        let rc = unsafe { libc::kill(-pgid, sig) };
        if rc != 0 {
            let err = std::io::Error::last_os_error();
            warn!(pgid, sig, error = %err, "failed to signal process group");
        }
    }
}

#[cfg(not(unix))]
fn kill_child_group(_pid: Option<u32>, _sigkill: bool) {}

/// Run `pg_dump` according to `req` using the supplied [`CommandRunner`].
pub async fn run_pg_dump<R: CommandRunner + ?Sized>(
    runner: &R,
    req: &DumpRequest,
    cancel: &CancellationToken,
) -> Result<()> {
    let args = build_pg_dump_args(req);
    let env = pgpassword_env(&req.source);
    runner.run("pg_dump", &args, &env, cancel).await
}

/// Build the `PGPASSWORD` env override for the given endpoint, if any.
pub(crate) fn pgpassword_env(ep: &EndpointConfig) -> Vec<(String, String)> {
    if ep.password.is_empty() {
        Vec::new()
    } else {
        vec![("PGPASSWORD".into(), ep.password.clone())]
    }
}

/// Returns `true` if `path` looks like a directory-format dump (used for
/// parallel restore).
pub fn is_directory_dump(path: &Path) -> bool {
    path.is_dir()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    fn sample_endpoint() -> EndpointConfig {
        EndpointConfig::parse("postgresql://alice:s3cret@db.example:5433/app").unwrap()
    }

    fn base_request() -> DumpRequest {
        DumpRequest {
            source: sample_endpoint(),
            exclude_table_data: Vec::new(),
            scope: DumpScope::All,
            jobs: 4,
            snapshot: None,
            schemas: Vec::new(),
            tables: Vec::new(),
            exclude_schemas: Vec::new(),
            exclude_tables: Vec::new(),
            output_path: PathBuf::from("/tmp/dump.bin"),
            format: DumpFormat::Custom,
            no_publications: true,
            no_subscriptions: true,
            compress: None,
            no_sync: true,
            no_comments: true,
            no_security_labels: true,
            no_table_access_method: false,
        }
    }

    #[test]
    fn dump_format_flag_mapping() {
        assert_eq!(DumpFormat::Custom.flag(), "c");
        assert_eq!(DumpFormat::Plain.flag(), "p");
        assert_eq!(DumpFormat::Directory.flag(), "d");
    }

    #[test]
    fn build_args_includes_endpoint_components() {
        let args = build_pg_dump_args(&base_request());
        assert!(args.iter().any(|a| a == "--host"));
        assert!(args.iter().any(|a| a == "db.example"));
        assert!(args.iter().any(|a| a == "5433"));
        assert!(args.iter().any(|a| a == "app"));
        assert!(args.iter().any(|a| a == "alice"));
        assert!(args.iter().any(|a| a == "--format"));
        assert!(args.iter().any(|a| a == "c"));
    }

    #[test]
    fn build_args_includes_jobs_only_for_directory_format() {
        let mut req = base_request();
        req.format = DumpFormat::Custom;
        let args = build_pg_dump_args(&req);
        assert!(!args.iter().any(|a| a == "--jobs"));

        req.format = DumpFormat::Directory;
        let args = build_pg_dump_args(&req);
        assert!(args.iter().any(|a| a == "--jobs"));
        assert!(args.iter().any(|a| a == "4"));
    }

    #[test]
    fn build_args_passes_snapshot_to_pg_dump() {
        let mut req = base_request();
        req.snapshot = Some("00000003-0000000A-1".into());
        let args = build_pg_dump_args(&req);
        assert!(args.iter().any(|a| a == "--snapshot=00000003-0000000A-1"));
    }

    #[test]
    fn build_args_appends_schema_only_flag() {
        let mut req = base_request();
        req.scope = DumpScope::SchemaOnly;
        let args = build_pg_dump_args(&req);
        assert!(args.iter().any(|a| a == "--schema-only"));
    }

    #[test]
    fn build_args_appends_schemas_and_tables() {
        let mut req = base_request();
        req.schemas = vec!["public".into(), "app".into()];
        req.tables = vec!["public.users".into()];
        let args = build_pg_dump_args(&req);
        assert!(args.iter().any(|a| a == "--schema=public"));
        assert!(args.iter().any(|a| a == "--schema=app"));
        assert!(args.iter().any(|a| a == "--table=public.users"));
    }

    #[test]
    fn build_args_appends_exclude_schemas_and_tables() {
        let mut req = base_request();
        req.exclude_schemas = vec!["audit".into(), "tenant_z".into()];
        req.exclude_tables = vec!["app.scratch".into()];
        let args = build_pg_dump_args(&req);
        assert!(args.iter().any(|a| a == "--exclude-schema=audit"));
        assert!(args.iter().any(|a| a == "--exclude-schema=tenant_z"));
        assert!(args.iter().any(|a| a == "--exclude-table=app.scratch"));
    }

    #[test]
    fn build_args_includes_no_publications_when_enabled() {
        let req = base_request(); // defaults to no_publications=true
        let args = build_pg_dump_args(&req);
        assert!(args.iter().any(|a| a == "--no-publications"));
        assert!(args.iter().any(|a| a == "--no-subscriptions"));
    }

    #[test]
    fn build_args_omits_no_publications_when_disabled() {
        let mut req = base_request();
        req.no_publications = false;
        req.no_subscriptions = false;
        let args = build_pg_dump_args(&req);
        assert!(!args.iter().any(|a| a == "--no-publications"));
        assert!(!args.iter().any(|a| a == "--no-subscriptions"));
    }

    #[test]
    fn build_args_includes_no_sync_by_default() {
        let args = build_pg_dump_args(&base_request());
        assert!(
            args.iter().any(|a| a == "--no-sync"),
            "dump archive is transient — fsync is pure overhead"
        );
    }

    #[test]
    fn build_args_omits_no_sync_when_disabled() {
        let mut req = base_request();
        req.no_sync = false;
        let args = build_pg_dump_args(&req);
        assert!(!args.iter().any(|a| a == "--no-sync"));
    }

    #[test]
    fn build_args_passes_compress_spec_when_set() {
        let mut req = base_request();
        req.compress = Some("lz4:1".into());
        let args = build_pg_dump_args(&req);
        assert!(args.iter().any(|a| a == "--compress=lz4:1"));
    }

    #[test]
    fn build_args_omits_compress_when_unset() {
        let args = build_pg_dump_args(&base_request());
        assert!(!args.iter().any(|a| a.starts_with("--compress=")));
    }

    #[test]
    fn build_args_includes_no_comments_by_default() {
        let args = build_pg_dump_args(&base_request());
        assert!(args.iter().any(|a| a == "--no-comments"));
    }

    #[test]
    fn build_args_omits_no_comments_when_disabled() {
        let mut req = base_request();
        req.no_comments = false;
        let args = build_pg_dump_args(&req);
        assert!(!args.iter().any(|a| a == "--no-comments"));
    }

    #[test]
    fn build_args_includes_no_security_labels_by_default() {
        let args = build_pg_dump_args(&base_request());
        assert!(args.iter().any(|a| a == "--no-security-labels"));
    }

    #[test]
    fn build_args_omits_no_security_labels_when_disabled() {
        let mut req = base_request();
        req.no_security_labels = false;
        let args = build_pg_dump_args(&req);
        assert!(!args.iter().any(|a| a == "--no-security-labels"));
    }

    #[test]
    fn build_args_omits_no_table_access_method_by_default() {
        let args = build_pg_dump_args(&base_request());
        assert!(!args.iter().any(|a| a == "--no-table-access-method"));
    }

    #[test]
    fn build_args_includes_no_table_access_method_when_enabled() {
        let mut req = base_request();
        req.no_table_access_method = true;
        let args = build_pg_dump_args(&req);
        assert!(args.iter().any(|a| a == "--no-table-access-method"));
    }

    /// Simple [`CommandRunner`] used in tests to assert on the command line
    /// produced by the higher-level helpers.
    type RunCall = (String, Vec<String>, Vec<(String, String)>);

    #[derive(Debug, Default, Clone)]
    struct RecordingRunner {
        calls: Arc<Mutex<Vec<RunCall>>>,
    }

    impl RecordingRunner {
        async fn calls(&self) -> Vec<RunCall> {
            self.calls.lock().await.clone()
        }
    }

    #[async_trait]
    impl CommandRunner for RecordingRunner {
        async fn run(
            &self,
            program: &str,
            args: &[String],
            env: &[(String, String)],
            _cancel: &CancellationToken,
        ) -> Result<()> {
            self.calls
                .lock()
                .await
                .push((program.to_string(), args.to_vec(), env.to_vec()));
            Ok(())
        }
    }

    #[tokio::test]
    async fn run_pg_dump_invokes_runner_with_pgpassword() {
        let runner = RecordingRunner::default();
        run_pg_dump(&runner, &base_request(), &CancellationToken::new())
            .await
            .unwrap();
        let calls = runner.calls().await;
        assert_eq!(calls.len(), 1);
        let (program, _args, env) = &calls[0];
        assert_eq!(program, "pg_dump");
        assert!(env.iter().any(|(k, v)| k == "PGPASSWORD" && v == "s3cret"));
    }

    #[tokio::test]
    async fn run_pg_dump_omits_pgpassword_when_no_password() {
        let runner = RecordingRunner::default();
        let mut req = base_request();
        req.source = EndpointConfig::parse("postgresql://u@h/db").unwrap();
        run_pg_dump(&runner, &req, &CancellationToken::new())
            .await
            .unwrap();
        let calls = runner.calls().await;
        assert!(calls[0].2.is_empty());
    }

    #[tokio::test]
    async fn tokio_runner_returns_cancelled_when_token_fires_mid_run() {
        // `sleep 30` is plenty of time for the cancel to land first.
        let cancel = CancellationToken::new();
        let cancel2 = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            cancel2.cancel();
        });
        let runner = TokioCommandRunner;
        let err = runner
            .run("sleep", &["30".into()], &[], &cancel)
            .await
            .unwrap_err();
        assert!(matches!(err, MigrationError::Cancelled));
    }

    #[test]
    fn pgpassword_env_returns_password_when_present() {
        let ep = sample_endpoint();
        let env = pgpassword_env(&ep);
        assert_eq!(env.len(), 1);
        assert_eq!(env[0].0, "PGPASSWORD");
        assert_eq!(env[0].1, "s3cret");
    }

    #[test]
    fn pgpassword_env_returns_empty_when_no_password() {
        let ep = EndpointConfig::parse("postgresql://u@h/db").unwrap();
        let env = pgpassword_env(&ep);
        assert!(env.is_empty());
    }

    #[test]
    fn is_directory_dump_false_for_nonexistent_path() {
        assert!(!is_directory_dump(std::path::Path::new(
            "/nonexistent/path/dump"
        )));
    }

    #[test]
    fn is_directory_dump_true_for_actual_directory() {
        let dir = tempfile::tempdir().unwrap();
        assert!(is_directory_dump(dir.path()));
    }

    #[test]
    fn is_directory_dump_false_for_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("dump.bin");
        std::fs::write(&file, b"data").unwrap();
        assert!(!is_directory_dump(&file));
    }

    #[test]
    fn build_args_appends_data_only_flag() {
        let mut req = base_request();
        req.scope = DumpScope::DataOnly;
        let args = build_pg_dump_args(&req);
        assert!(args.iter().any(|a| a == "--data-only"));
    }

    #[test]
    fn build_args_no_scope_flag_for_all() {
        let req = base_request(); // scope = DumpScope::All
        let args = build_pg_dump_args(&req);
        assert!(!args.iter().any(|a| a == "--schema-only"));
        assert!(!args.iter().any(|a| a == "--data-only"));
    }

    #[test]
    fn build_args_directory_format_single_job_omits_jobs() {
        let mut req = base_request();
        req.format = DumpFormat::Directory;
        req.jobs = 1;
        let args = build_pg_dump_args(&req);
        assert!(!args.iter().any(|a| a == "--jobs"));
    }

    #[tokio::test]
    async fn tokio_runner_succeeds_for_true_command() {
        let runner = TokioCommandRunner;
        runner
            .run("true", &[], &[], &CancellationToken::new())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn tokio_runner_fails_for_false_command() {
        let runner = TokioCommandRunner;
        let err = runner
            .run("false", &[], &[], &CancellationToken::new())
            .await
            .unwrap_err();
        assert!(matches!(err, MigrationError::ExternalCommand { .. }));
    }

    #[tokio::test]
    async fn tokio_runner_returns_error_for_nonexistent_binary() {
        let runner = TokioCommandRunner;
        let err = runner
            .run(
                "definitely_not_a_real_binary_xyzzy",
                &[],
                &[],
                &CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, MigrationError::ExternalCommand { .. }));
    }

    #[test]
    fn dump_format_flag_values() {
        assert_eq!(DumpFormat::Custom.flag(), "c");
        assert_eq!(DumpFormat::Plain.flag(), "p");
        assert_eq!(DumpFormat::Directory.flag(), "d");
    }

    #[test]
    fn build_args_includes_snapshot_when_set() {
        let mut req = base_request();
        req.snapshot = Some("00000003-deadbeef".into());
        let args = build_pg_dump_args(&req);
        assert!(args.iter().any(|a| a == "--snapshot=00000003-deadbeef"));
    }

    #[test]
    fn build_args_includes_schemas() {
        let mut req = base_request();
        req.schemas = vec!["public".into(), "app".into()];
        let args = build_pg_dump_args(&req);
        assert!(args.iter().any(|a| a == "--schema=public"));
        assert!(args.iter().any(|a| a == "--schema=app"));
    }

    #[test]
    fn build_args_includes_tables() {
        let mut req = base_request();
        req.tables = vec!["public.users".into()];
        let args = build_pg_dump_args(&req);
        assert!(args.iter().any(|a| a == "--table=public.users"));
    }

    #[test]
    fn build_args_includes_exclude_schemas() {
        let mut req = base_request();
        req.exclude_schemas = vec!["audit".into()];
        let args = build_pg_dump_args(&req);
        assert!(args.iter().any(|a| a == "--exclude-schema=audit"));
    }

    #[test]
    fn build_args_includes_exclude_tables() {
        let mut req = base_request();
        req.exclude_tables = vec!["public.large_table".into()];
        let args = build_pg_dump_args(&req);
        assert!(args
            .iter()
            .any(|a| a == "--exclude-table=public.large_table"));
    }

    #[test]
    fn build_args_includes_compress_spec() {
        let mut req = base_request();
        req.compress = Some("zstd:3".into());
        let args = build_pg_dump_args(&req);
        assert!(args.iter().any(|a| a == "--compress=zstd:3"));
    }

    #[test]
    fn build_args_includes_no_publications() {
        let mut req = base_request();
        req.no_publications = true;
        let args = build_pg_dump_args(&req);
        assert!(args.iter().any(|a| a == "--no-publications"));
    }

    #[test]
    fn build_args_includes_no_subscriptions() {
        let mut req = base_request();
        req.no_subscriptions = true;
        let args = build_pg_dump_args(&req);
        assert!(args.iter().any(|a| a == "--no-subscriptions"));
    }

    #[test]
    fn build_args_omits_no_publications_when_false() {
        let mut req = base_request();
        req.no_publications = false;
        let args = build_pg_dump_args(&req);
        assert!(!args.iter().any(|a| a == "--no-publications"));
    }

    #[test]
    fn build_args_directory_format_with_parallel_jobs() {
        let mut req = base_request();
        req.format = DumpFormat::Directory;
        req.jobs = 4;
        let args = build_pg_dump_args(&req);
        assert!(args.contains(&"--jobs".to_string()));
        assert!(args.contains(&"4".to_string()));
    }

    #[test]
    fn tokio_command_runner_debug() {
        let r = TokioCommandRunner;
        let dbg = format!("{:?}", r);
        assert!(dbg.contains("TokioCommandRunner"));
    }
}
