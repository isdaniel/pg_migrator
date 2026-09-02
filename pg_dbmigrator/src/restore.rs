//! Wrapper around `pg_restore` (and `psql` for plain SQL dumps).

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::config::EndpointConfig;
use crate::dump::{is_directory_dump, pgpassword_env, CommandRunner};
use crate::error::{MigrationError, Result};

/// Restore phase, mapped to `pg_restore --section=<value>`.
///
/// The standard pg_dump-best-practice ordering is
/// `PreData` (CREATE TABLE / TYPE / FUNCTION DDL with no indexes) →
/// `Data` (COPY of every table) → `PostData` (PRIMARY KEY, CHECK,
/// FOREIGN KEY, INDEX, TRIGGER). Splitting the restore lets the bulk
/// `Data` phase run without index maintenance, and lets the `PostData`
/// phase rebuild every index in parallel against fully-loaded tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RestoreSection {
    /// Schema DDL only (no indexes, no constraints, no triggers).
    PreData,
    /// `COPY` of every table's contents.
    Data,
    /// Indexes, primary / foreign keys, check constraints, triggers.
    PostData,
}

impl RestoreSection {
    /// Returns the value to pass to `--section=<value>`.
    pub fn flag(self) -> &'static str {
        match self {
            Self::PreData => "pre-data",
            Self::Data => "data",
            Self::PostData => "post-data",
        }
    }
}

/// Coarse classification of a single `pg_restore: error:` line, used to
/// produce an actionable summary on exit-1.
///
/// The categories are ordered from "most concerning" to "most likely
/// cosmetic" so a simple sort-by-discriminant gives the right priority
/// when listing them in a report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RestoreErrorCategory {
    /// `COPY` failure, NOT-NULL / CHECK / FK violation, syntax error in
    /// row data, etc. — strongly suggests user data was lost. The
    /// operator MUST review before cutover.
    DataLoss,
    /// Object the dump references already exists on the target. Usually
    /// indicates a stale target or a missed `--clean`. Worth a look but
    /// rarely a data-loss issue.
    Duplicate,
    /// `permission denied`, `must be owner`, `must be superuser`,
    /// `role "X" does not exist`. On managed Postgres (Azure / AWS / GCP) these are typically inevitable: the source's `OWNER TO rdsadmin` / `GRANT ... TO azure_pg_admin` lines have no analogue
    /// on the target. User data is unaffected.
    Privilege,
    /// Extension internal state (`COMMENT ON EXTENSION`,
    /// `pg_extension_config_dump`, locked `pg_cron` / `pg_stat_statements`
    /// metadata, etc.). On managed Postgres these are typically
    /// inevitable; user data is unaffected.
    Extension,
    /// Anything we couldn't classify — operator should read the line.
    Other,
}

impl RestoreErrorCategory {
    /// Human-readable short name suitable for log output.
    pub fn label(self) -> &'static str {
        match self {
            Self::DataLoss => "data-loss",
            Self::Duplicate => "duplicate",
            Self::Privilege => "privilege",
            Self::Extension => "extension",
            Self::Other => "other",
        }
    }

    /// One-line plain-English explanation suitable for the summary.
    pub fn description(self) -> &'static str {
        match self {
            Self::DataLoss => "COPY/row-level errors — likely user-data loss",
            Self::Duplicate => "object already exists on target",
            Self::Privilege => {
                "role / owner / permission errors (typically cosmetic on managed PG)"
            }
            Self::Extension => "extension internal state (typically cosmetic on managed PG)",
            Self::Other => "unclassified — review the raw line",
        }
    }
}

/// Strip the `pg_restore: error: ` prefix and trailing whitespace from a
/// pg_restore stderr line. Returns `None` if the line is not an error
/// line. Public for unit tests and downstream tools.
pub fn extract_pg_restore_error_text(line: &str) -> Option<&str> {
    let trimmed = line.trim_end();
    let after_prefix = trimmed.strip_prefix("pg_restore: error: ")?;
    Some(after_prefix.trim_start())
}

/// Bucket a pg_restore error message into a [`RestoreErrorCategory`].
///
/// The message must already be stripped of the `pg_restore: error: `
/// prefix (use [`extract_pg_restore_error_text`] first).
pub fn classify_pg_restore_error(error_text: &str) -> RestoreErrorCategory {
    let l = error_text.to_ascii_lowercase();

    // DATA LOSS first — these are the only category that should ever
    // block a cutover. Look for both COPY-side errors and constraint
    // violations that imply user rows didn't make it.
    if l.contains("error:  copy ")
        || l.contains("error: copy ")
        || l.contains("invalid input syntax")
        || l.contains("violates not-null")
        || l.contains("violates check constraint")
        || l.contains("violates foreign key")
        || l.contains("violates unique constraint")
        || l.contains("value too long for type")
        || l.contains("out of range")
    {
        return RestoreErrorCategory::DataLoss;
    }

    // Already-exists / duplicate-key errors imply the target wasn't
    // clean. Not data-loss per se but worth flagging separately.
    if l.contains("already exists") || l.contains("duplicate key value") {
        return RestoreErrorCategory::Duplicate;
    }

    // Privilege / role issues. Order matters: `permission denied for
    // extension X` should be classified as Privilege (it's a GRANT
    // issue), not Extension.
    if l.contains("permission denied")
        || l.contains("must be owner")
        || l.contains("must be superuser")
        || l.contains("role \"")
        || l.contains("user mapping")
    {
        return RestoreErrorCategory::Privilege;
    }

    // Extension internal state.
    if l.contains("extension ") || l.contains("pg_extension") || l.contains("for extension") {
        return RestoreErrorCategory::Extension;
    }

    RestoreErrorCategory::Other
}

/// Bounded structured summary of a `pg_restore` run, populated by
/// [`ingest_pg_restore_stderr_line`] as stderr is teed.
///
/// Memory is bounded to ~5 KiB per category so a 50 000-error run does
/// not blow up the migrator's RSS.
#[derive(Debug, Default, Clone)]
pub struct RestoreErrorSummary {
    /// Number of error lines we captured per category.
    pub counts: BTreeMap<RestoreErrorCategory, usize>,
    /// First few sample lines per category (capped at 5 each).
    pub samples: BTreeMap<RestoreErrorCategory, Vec<String>>,
    /// `errors ignored on restore: N` value reported by `pg_restore`
    /// itself, if observed.
    pub errors_ignored_reported: Option<u32>,
    /// Total error lines captured (== sum of counts).
    pub total_captured: usize,
}

impl RestoreErrorSummary {
    /// Maximum number of sample lines retained per category.
    pub const MAX_SAMPLES_PER_CATEGORY: usize = 5;

    /// Whether any data-loss-suspect error was captured.
    pub fn has_data_loss(&self) -> bool {
        self.counts
            .get(&RestoreErrorCategory::DataLoss)
            .copied()
            .unwrap_or(0)
            > 0
    }

    /// Whether the summary contains any captured errors at all.
    pub fn is_empty(&self) -> bool {
        self.total_captured == 0
    }

    /// One-line verdict suitable for the warn log.
    pub fn verdict(&self) -> &'static str {
        if self.has_data_loss() {
            "REVIEW REQUIRED — data-loss-suspect errors observed; do NOT cut over until investigated"
        } else if self.total_captured == 0 {
            "no errors captured"
        } else {
            "likely safe — privilege/extension/duplicate only; verify row counts before cutover"
        }
    }

    /// Render a human-readable multi-line block for embedding in a log
    /// message or error string.
    pub fn render_report(&self) -> String {
        if self.is_empty() && self.errors_ignored_reported.is_none() {
            return "pg_restore exited non-zero but no error lines were captured".into();
        }

        let mut out = String::new();
        out.push_str("--- pg_restore error summary ---\n");
        out.push_str(&format!("total errors captured: {}", self.total_captured));
        if let Some(reported) = self.errors_ignored_reported {
            out.push_str(&format!(
                " (pg_restore reports: errors ignored on restore: {reported})"
            ));
        }
        out.push('\n');

        out.push_str("breakdown by category:\n");
        // Iterate in BTreeMap key order — categories are ordered from
        // most concerning to most cosmetic.
        for (cat, n) in &self.counts {
            if *n == 0 {
                continue;
            }
            out.push_str(&format!(
                "  - {n:>4} {label:<10} — {desc}\n",
                label = cat.label(),
                desc = cat.description()
            ));
        }

        out.push_str("samples:\n");
        for (cat, lines) in &self.samples {
            if lines.is_empty() {
                continue;
            }
            out.push_str(&format!("  [{}]\n", cat.label()));
            for l in lines {
                out.push_str(&format!("    {l}\n"));
            }
        }

        out.push_str(&format!("verdict: {}", self.verdict()));
        out
    }
}

/// Process one stderr line from `pg_restore`. Updates the summary in
/// place. Pure function — fully unit-testable.
pub fn ingest_pg_restore_stderr_line(line: &str, summary: &mut RestoreErrorSummary) {
    let trimmed = line.trim_end();

    // The `errors ignored on restore: N` summary is a warning, not an
    // error. Capture the reported count so we can cross-check our own.
    if let Some(rest) = trimmed.strip_prefix("pg_restore: warning: errors ignored on restore: ") {
        if let Ok(n) = rest.trim().parse::<u32>() {
            summary.errors_ignored_reported = Some(n);
        }
        return;
    }

    let Some(error_text) = extract_pg_restore_error_text(trimmed) else {
        return;
    };

    let cat = classify_pg_restore_error(error_text);
    *summary.counts.entry(cat).or_insert(0) += 1;
    summary.total_captured += 1;
    let bucket = summary.samples.entry(cat).or_default();
    if bucket.len() < RestoreErrorSummary::MAX_SAMPLES_PER_CATEGORY {
        bucket.push(error_text.to_string());
    }
}

/// Description of a restore invocation.
#[derive(Debug, Clone)]
pub struct RestoreRequest {
    /// Target endpoint to restore into.
    pub target: EndpointConfig,
    /// Path to the dump archive (or directory) produced by `pg_dump`.
    pub input_path: PathBuf,
    /// Number of parallel jobs to use (`--jobs`).
    pub jobs: usize,
    /// If `true`, pass `--clean --if-exists` so the target is reset first.
    pub clean: bool,
    /// If `true`, pass `--no-owner` (recommended when the target uses
    /// different role names).
    pub no_owner: bool,
    /// If `true`, pass `--no-acl` (skip GRANT/REVOKE statements). Strongly
    /// recommended for cross-server migrations where roles referenced by the
    /// source ACLs may not exist on the target.
    pub no_acl: bool,
    /// If `true`, treat a `pg_restore` exit-1 (the conventional "completed
    /// with errors" signal — see the `errors ignored on restore: N` warning)
    /// as a non-fatal warning rather than aborting the migration.
    ///
    /// Use case: cross-server migrations where the source has installed
    /// extensions whose internal state cannot be re-created on the target
    /// (e.g. Azure-reserved `azure` / `pgaadauth` extensions, `pg_cron`
    /// metadata tables that only `azure_pg_admin` can write to). The user
    /// data itself restores correctly — only extension metadata fails.
    ///
    /// Default `false` (fail-fast). Enable explicitly via the CLI's
    /// `--allow-restore-errors` flag.
    pub tolerate_errors: bool,
    /// If `Some`, restrict this restore to a single section
    /// (`--section=<flag>`). The split-section orchestration in
    /// [`run_pg_restore_in_sections`] sets this to PreData, then Data,
    /// then PostData on three consecutive calls. Leave `None` for a
    /// single all-in-one restore (the legacy behaviour).
    pub section: Option<RestoreSection>,
}

/// Build the argv vector for `pg_restore` based on the given request.
pub fn build_pg_restore_args(req: &RestoreRequest) -> Vec<String> {
    let mut args: Vec<String> = vec![
        "--no-password".into(),
        "--verbose".into(),
        "--host".into(),
        req.target.host.clone(),
        "--port".into(),
        req.target.port.to_string(),
        "--username".into(),
        req.target.user.clone(),
        "--dbname".into(),
        req.target.database.clone(),
    ];

    if req.jobs > 1 && is_directory_dump(&req.input_path) {
        args.push("--jobs".into());
        args.push(req.jobs.to_string());
    }
    // `--clean --if-exists` issues DROPs that only belong in pre-data; on
    // a section-restricted Data/PostData call they would happily drop the
    // freshly-loaded data. Limit the flag to "no section" or "pre-data".
    if req.clean && matches!(req.section, None | Some(RestoreSection::PreData)) {
        args.push("--clean".into());
        args.push("--if-exists".into());
    }
    if req.no_owner {
        args.push("--no-owner".into());
    }
    if req.no_acl {
        args.push("--no-acl".into());
    }
    if let Some(section) = req.section {
        args.push(format!("--section={}", section.flag()));
    }

    args.push(req.input_path.to_string_lossy().into_owned());
    args
}

/// Run `pg_restore` for the given request via the supplied runner.
pub async fn run_pg_restore<R: CommandRunner + ?Sized>(
    runner: &R,
    req: &RestoreRequest,
    cancel: &CancellationToken,
) -> Result<()> {
    info!(target = %req.target.redacted(), "running pg_restore");
    let args = build_pg_restore_args(req);
    let env = pgpassword_env(&req.target);
    match runner.run("pg_restore", &args, &env, cancel).await {
        Ok(()) => Ok(()),
        Err(e) if req.tolerate_errors && is_restore_partial_failure(&e) => {
            // `e` already carries the categorized restore-error report
            // produced by `TokioCommandRunner`'s stderr capture (look
            // for the "--- pg_restore error summary ---" block).
            // Including it via `error = %e` gives the operator a
            // ready-made go/no-go verdict instead of just an exit code.
            tracing::warn!(
                error = %e,
                "pg_restore exited non-zero but tolerate_errors=true; \
                 treating as warning. Inspect the error summary above: \
                 if `data-loss` count is zero the failure is almost \
                 certainly cosmetic (privilege/extension on managed PG); \
                 otherwise re-run without --allow-restore-errors."
            );
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// Heuristic: `pg_restore` exit 1 is the conventional "completed with
/// errors" signal. We don't try to distinguish *which* errors occurred — the
/// caller has explicitly opted in via `tolerate_errors`. Any other failure
/// kind (spawn error, signal kill, etc.) still aborts.
fn is_restore_partial_failure(err: &MigrationError) -> bool {
    matches!(
        err,
        MigrationError::ExternalCommand { command, .. } if command == "pg_restore"
    )
}

/// Canonical order for a split-section restore: schema, then table data,
/// then indexes and constraints.
///
/// Shared with [`crate::orchestrator`], which runs the same sequence but
/// emits a progress event per section, so the order lives in exactly one
/// place.
pub const RESTORE_SECTIONS: [RestoreSection; 3] = [
    RestoreSection::PreData,
    RestoreSection::Data,
    RestoreSection::PostData,
];

/// Run `pg_restore` three times — `--section=pre-data`, then `data`, then
/// `post-data`.
///
/// Why split: the data section (`COPY` of every table) is by far the
/// hottest phase. Restoring without indexes / FK constraints lets `COPY`
/// run at raw I/O speed; the post-data section then rebuilds every index
/// in parallel (`--jobs N`) on already-warm tables. On schemas with many
/// secondary indexes this is typically 30–60 % faster than the
/// all-in-one restore.
///
/// Only the pre-data call honours `req.clean`; the Data and PostData
/// calls reuse the same archive but never re-issue DROPs.
pub async fn run_pg_restore_in_sections<R: CommandRunner + ?Sized>(
    runner: &R,
    base_req: &RestoreRequest,
    cancel: &CancellationToken,
) -> Result<()> {
    for section in RESTORE_SECTIONS {
        if cancel.is_cancelled() {
            return Err(MigrationError::Cancelled);
        }
        info!(?section, "running pg_restore section");
        let mut req = base_req.clone();
        req.section = Some(section);
        run_pg_restore(runner, &req, cancel).await?;
    }
    Ok(())
}
/// Convenience helper: run a plain SQL file through `psql` (used when the
/// dump format is [`crate::dump::DumpFormat::Plain`]).
pub async fn run_psql_file<R: CommandRunner + ?Sized>(
    runner: &R,
    target: &EndpointConfig,
    sql_path: &std::path::Path,
    cancel: &CancellationToken,
) -> Result<()> {
    let args: Vec<String> = vec![
        "--no-password".into(),
        "--single-transaction".into(),
        "--set".into(),
        "ON_ERROR_STOP=1".into(),
        "--host".into(),
        target.host.clone(),
        "--port".into(),
        target.port.to_string(),
        "--username".into(),
        target.user.clone(),
        "--dbname".into(),
        target.database.clone(),
        "--file".into(),
        sql_path.to_string_lossy().into_owned(),
    ];

    runner
        .run("psql", &args, &pgpassword_env(target), cancel)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EndpointConfig;
    use async_trait::async_trait;

    fn sample_request() -> RestoreRequest {
        RestoreRequest {
            target: EndpointConfig::parse("postgresql://bob:pw@target.example:6432/app").unwrap(),
            input_path: PathBuf::from("/tmp/dump.bin"),
            jobs: 4,
            clean: true,
            no_owner: true,
            no_acl: true,
            tolerate_errors: false,
            section: None,
        }
    }

    #[test]
    fn section_flag_mapping() {
        assert_eq!(RestoreSection::PreData.flag(), "pre-data");
        assert_eq!(RestoreSection::Data.flag(), "data");
        assert_eq!(RestoreSection::PostData.flag(), "post-data");
    }

    #[test]
    fn build_args_includes_section_flag_when_set() {
        let mut req = sample_request();
        req.section = Some(RestoreSection::PostData);
        let args = build_pg_restore_args(&req);
        assert!(args.iter().any(|a| a == "--section=post-data"));
    }

    #[test]
    fn build_args_omits_clean_for_data_and_postdata_sections() {
        let mut req = sample_request(); // clean=true
        req.section = Some(RestoreSection::Data);
        let args = build_pg_restore_args(&req);
        assert!(
            !args.iter().any(|a| a == "--clean"),
            "data section must not re-issue DROPs"
        );

        req.section = Some(RestoreSection::PostData);
        let args = build_pg_restore_args(&req);
        assert!(!args.iter().any(|a| a == "--clean"));
    }

    #[test]
    fn build_args_keeps_clean_for_predata_section() {
        let mut req = sample_request();
        req.section = Some(RestoreSection::PreData);
        let args = build_pg_restore_args(&req);
        assert!(args.iter().any(|a| a == "--clean"));
        assert!(args.iter().any(|a| a == "--if-exists"));
    }

    #[test]
    fn build_args_includes_clean_and_no_owner_flags() {
        let args = build_pg_restore_args(&sample_request());
        assert!(args.iter().any(|a| a == "--clean"));
        assert!(args.iter().any(|a| a == "--if-exists"));
        assert!(args.iter().any(|a| a == "--no-owner"));
        assert!(args.iter().any(|a| a == "--no-acl"));
        assert!(args.iter().any(|a| a == "target.example"));
        assert!(args.iter().any(|a| a == "6432"));
    }

    #[test]
    fn build_args_omits_jobs_for_non_directory_dump() {
        // Path doesn't exist as a directory, so `--jobs` should not be added.
        let args = build_pg_restore_args(&sample_request());
        assert!(!args.iter().any(|a| a == "--jobs"));
    }

    #[test]
    fn build_args_skips_clean_when_not_requested() {
        let mut req = sample_request();
        req.clean = false;
        req.no_owner = false;
        req.no_acl = false;
        let args = build_pg_restore_args(&req);
        assert!(!args.iter().any(|a| a == "--clean"));
        assert!(!args.iter().any(|a| a == "--no-owner"));
        assert!(!args.iter().any(|a| a == "--no-acl"));
    }

    #[test]
    fn build_args_input_path_is_last() {
        let args = build_pg_restore_args(&sample_request());
        assert_eq!(args.last().unwrap(), "/tmp/dump.bin");
    }

    #[test]
    fn is_restore_partial_failure_matches_pg_restore_external_failure() {
        let e = MigrationError::external("pg_restore", "exited with status exit status: 1");
        assert!(is_restore_partial_failure(&e));
    }

    #[test]
    fn is_restore_partial_failure_rejects_other_commands() {
        let e = MigrationError::external("pg_dump", "exit 1");
        assert!(!is_restore_partial_failure(&e));
    }

    #[test]
    fn is_restore_partial_failure_rejects_other_error_kinds() {
        assert!(!is_restore_partial_failure(&MigrationError::config("nope")));
        assert!(!is_restore_partial_failure(&MigrationError::Cancelled));
    }

    /// Runner that always fails with the given error. Used to drive
    /// `run_pg_restore`'s `tolerate_errors` branch.
    #[derive(Debug)]
    struct FailingRunner {
        program_to_fail: String,
    }

    #[async_trait]
    impl CommandRunner for FailingRunner {
        async fn run(
            &self,
            program: &str,
            _args: &[String],
            _env: &[(String, String)],
            _cancel: &CancellationToken,
        ) -> Result<()> {
            Err(MigrationError::external(
                self.program_to_fail.clone(),
                format!("simulated failure for {program}"),
            ))
        }
    }

    #[tokio::test]
    async fn run_pg_restore_tolerates_failure_when_flag_set() {
        let runner = FailingRunner {
            program_to_fail: "pg_restore".into(),
        };
        let mut req = sample_request();
        req.tolerate_errors = true;
        run_pg_restore(&runner, &req, &CancellationToken::new())
            .await
            .expect("tolerate_errors=true should swallow pg_restore exit 1");
    }

    #[tokio::test]
    async fn run_pg_restore_propagates_failure_when_flag_unset() {
        let runner = FailingRunner {
            program_to_fail: "pg_restore".into(),
        };
        let req = sample_request(); // tolerate_errors = false
        let err = run_pg_restore(&runner, &req, &CancellationToken::new())
            .await
            .unwrap_err();
        assert!(matches!(err, MigrationError::ExternalCommand { .. }));
    }

    /// Records every spawned command and the `--section=` flag observed.
    #[derive(Debug, Default)]
    struct SectionRecordingRunner {
        sections: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait]
    impl CommandRunner for SectionRecordingRunner {
        async fn run(
            &self,
            _program: &str,
            args: &[String],
            _env: &[(String, String)],
            _cancel: &CancellationToken,
        ) -> Result<()> {
            let section = args
                .iter()
                .find(|a| a.starts_with("--section="))
                .cloned()
                .unwrap_or_else(|| "<none>".into());
            self.sections.lock().unwrap().push(section);
            Ok(())
        }
    }

    #[tokio::test]
    async fn run_pg_restore_in_sections_calls_pre_data_then_data_then_post_data() {
        let runner = SectionRecordingRunner::default();
        run_pg_restore_in_sections(&runner, &sample_request(), &CancellationToken::new())
            .await
            .expect("section restore should succeed");
        let observed = runner.sections.lock().unwrap().clone();
        assert_eq!(
            observed,
            vec![
                "--section=pre-data".to_string(),
                "--section=data".to_string(),
                "--section=post-data".to_string(),
            ]
        );
    }

    // ---------------------------------------------------------------
    // Pure-function tests for the pg_restore stderr classifier and the
    // accumulating summary. These run without spawning anything and
    // are the source of truth for the verdict that the warn log emits
    // on `--allow-restore-errors` paths.
    // ---------------------------------------------------------------

    #[test]
    fn extract_pg_restore_error_text_strips_prefix() {
        let line = "pg_restore: error: could not execute query: ERROR:  permission denied for extension pg_stat_statements";
        let got = extract_pg_restore_error_text(line);
        assert_eq!(
            got,
            Some(
                "could not execute query: ERROR:  permission denied for extension pg_stat_statements"
            )
        );
    }

    #[test]
    fn extract_pg_restore_error_text_returns_none_for_non_error_lines() {
        assert!(extract_pg_restore_error_text(
            "pg_restore: warning: errors ignored on restore: 16"
        )
        .is_none());
        assert!(extract_pg_restore_error_text("processing data for table public.users").is_none());
        assert!(extract_pg_restore_error_text("").is_none());
    }

    #[test]
    fn classify_recognises_data_loss_signals() {
        let cases = [
            "could not execute query: ERROR:  COPY orders, line 42: invalid input syntax for type integer",
            "ERROR:  null value in column \"id\" violates not-null constraint",
            "ERROR:  new row for relation \"t\" violates check constraint \"t_chk\"",
            "ERROR:  insert or update on table \"t\" violates foreign key constraint",
            "ERROR:  duplicate key value violates unique constraint",
            "ERROR:  value too long for type character varying(8)",
            "ERROR:  integer out of range",
        ];
        for c in cases {
            assert_eq!(
                classify_pg_restore_error(c),
                RestoreErrorCategory::DataLoss,
                "expected DataLoss for: {c}"
            );
        }
    }

    #[test]
    fn classify_recognises_privilege_signals() {
        let cases = [
            "could not execute query: ERROR:  permission denied for extension pg_stat_statements",
            "ERROR:  must be owner of extension pg_cron",
            "ERROR:  must be superuser to create event triggers",
            "ERROR:  role \"rdsadmin\" does not exist",
            "ERROR:  permission denied to create user mapping for \"x\"",
        ];
        for c in cases {
            assert_eq!(
                classify_pg_restore_error(c),
                RestoreErrorCategory::Privilege,
                "expected Privilege for: {c}"
            );
        }
    }

    #[test]
    fn classify_recognises_extension_state_signals() {
        let cases = [
            "could not execute query: ERROR:  extension \"pg_cron\" is not yet loaded",
            "could not execute query: ERROR:  pg_extension_config_dump entry not found",
        ];
        for c in cases {
            assert_eq!(
                classify_pg_restore_error(c),
                RestoreErrorCategory::Extension,
                "expected Extension for: {c}"
            );
        }
    }

    #[test]
    fn classify_recognises_duplicate_signals() {
        // Note: `duplicate key value violates unique constraint` is
        // classified as DataLoss (constraint violation). The plain
        // `already exists` is the duplicate cleanup signal.
        let cases = [
            "ERROR:  relation \"users\" already exists",
            "ERROR:  function \"f\" already exists with same argument types",
        ];
        for c in cases {
            assert_eq!(
                classify_pg_restore_error(c),
                RestoreErrorCategory::Duplicate,
                "expected Duplicate for: {c}"
            );
        }
    }

    #[test]
    fn classify_falls_back_to_other() {
        let cases = [
            "could not connect to database: too many clients",
            "deadlock detected",
        ];
        for c in cases {
            assert_eq!(
                classify_pg_restore_error(c),
                RestoreErrorCategory::Other,
                "expected Other for: {c}"
            );
        }
    }

    #[test]
    fn classify_is_case_insensitive() {
        assert_eq!(
            classify_pg_restore_error("Permission Denied for extension foo"),
            RestoreErrorCategory::Privilege
        );
    }

    #[test]
    fn ingest_accumulates_counts_and_caps_samples() {
        let mut s = RestoreErrorSummary::default();
        // 7 privilege errors — only first 5 should be retained.
        for i in 0..7 {
            ingest_pg_restore_stderr_line(
                &format!(
                    "pg_restore: error: could not execute query: ERROR:  permission denied for relation t{i}"
                ),
                &mut s,
            );
        }
        assert_eq!(s.total_captured, 7);
        assert_eq!(
            s.counts.get(&RestoreErrorCategory::Privilege).copied(),
            Some(7)
        );
        assert_eq!(
            s.samples
                .get(&RestoreErrorCategory::Privilege)
                .map(|v| v.len()),
            Some(RestoreErrorSummary::MAX_SAMPLES_PER_CATEGORY)
        );
    }

    #[test]
    fn ingest_captures_pg_restore_reported_count() {
        let mut s = RestoreErrorSummary::default();
        ingest_pg_restore_stderr_line("pg_restore: warning: errors ignored on restore: 16", &mut s);
        assert_eq!(s.errors_ignored_reported, Some(16));
        // Warning lines themselves don't bump the captured-error count.
        assert_eq!(s.total_captured, 0);
    }

    #[test]
    fn ingest_ignores_unrelated_lines() {
        let mut s = RestoreErrorSummary::default();
        ingest_pg_restore_stderr_line("processing data for table public.users", &mut s);
        ingest_pg_restore_stderr_line("", &mut s);
        ingest_pg_restore_stderr_line("pg_restore: connecting to database for restore", &mut s);
        assert!(s.is_empty());
        assert!(s.errors_ignored_reported.is_none());
    }

    #[test]
    fn summary_verdict_flags_data_loss() {
        let mut s = RestoreErrorSummary::default();
        ingest_pg_restore_stderr_line(
            "pg_restore: error: could not execute query: ERROR:  COPY orders, line 1: invalid input syntax",
            &mut s,
        );
        assert!(s.has_data_loss());
        assert!(s.verdict().contains("REVIEW REQUIRED"));
    }

    #[test]
    fn summary_verdict_says_likely_safe_for_cosmetic_errors() {
        let mut s = RestoreErrorSummary::default();
        ingest_pg_restore_stderr_line(
            "pg_restore: error: could not execute query: ERROR:  permission denied for extension pg_stat_statements",
            &mut s,
        );
        ingest_pg_restore_stderr_line(
            "pg_restore: error: could not execute query: ERROR:  must be owner of extension pg_cron",
            &mut s,
        );
        ingest_pg_restore_stderr_line("pg_restore: warning: errors ignored on restore: 2", &mut s);
        assert!(!s.has_data_loss());
        assert!(s.verdict().contains("likely safe"));
        assert_eq!(s.errors_ignored_reported, Some(2));
    }

    #[test]
    fn render_report_includes_breakdown_and_samples() {
        let mut s = RestoreErrorSummary::default();
        ingest_pg_restore_stderr_line(
            "pg_restore: error: could not execute query: ERROR:  permission denied for extension pg_stat_statements",
            &mut s,
        );
        ingest_pg_restore_stderr_line(
            "pg_restore: error: could not execute query: ERROR:  COPY t, line 1: invalid input syntax",
            &mut s,
        );
        ingest_pg_restore_stderr_line("pg_restore: warning: errors ignored on restore: 2", &mut s);

        let report = s.render_report();
        // Header + count + sources visible.
        assert!(report.contains("--- pg_restore error summary ---"));
        assert!(report.contains("total errors captured: 2"));
        assert!(report.contains("errors ignored on restore: 2"));
        // Both categories should appear with their labels.
        assert!(report.contains("data-loss"));
        assert!(report.contains("privilege"));
        // The data-loss verdict wins because that category exists.
        assert!(report.contains("REVIEW REQUIRED"));
    }

    #[test]
    fn render_report_handles_empty_summary() {
        let s = RestoreErrorSummary::default();
        let report = s.render_report();
        assert!(report.contains("no error lines were captured"));
    }

    #[test]
    fn restore_error_category_labels_are_unique() {
        let cats = [
            RestoreErrorCategory::DataLoss,
            RestoreErrorCategory::Duplicate,
            RestoreErrorCategory::Privilege,
            RestoreErrorCategory::Extension,
            RestoreErrorCategory::Other,
        ];
        let labels: Vec<&str> = cats.iter().map(|c| c.label()).collect();
        let mut deduped = labels.clone();
        deduped.sort();
        deduped.dedup();
        assert_eq!(labels.len(), deduped.len());
    }

    #[test]
    fn restore_error_category_descriptions_are_non_empty() {
        let cats = [
            RestoreErrorCategory::DataLoss,
            RestoreErrorCategory::Duplicate,
            RestoreErrorCategory::Privilege,
            RestoreErrorCategory::Extension,
            RestoreErrorCategory::Other,
        ];
        for c in cats {
            assert!(!c.description().is_empty());
            assert!(!c.label().is_empty());
        }
    }

    #[test]
    fn restore_section_serde_roundtrip() {
        let sections = [
            RestoreSection::PreData,
            RestoreSection::Data,
            RestoreSection::PostData,
        ];
        for s in sections {
            let json = serde_json::to_string(&s).unwrap();
            let back: RestoreSection = serde_json::from_str(&json).unwrap();
            assert_eq!(back, s);
        }
    }

    #[test]
    fn summary_no_errors_verdict() {
        let s = RestoreErrorSummary::default();
        assert_eq!(s.verdict(), "no errors captured");
    }

    #[test]
    fn summary_has_data_loss_false_when_empty() {
        let s = RestoreErrorSummary::default();
        assert!(!s.has_data_loss());
    }

    /// Runner that records calls (used for `run_psql_file` tests).
    type RunCall = (String, Vec<String>, Vec<(String, String)>);

    #[derive(Debug, Default)]
    struct RecordingRunner {
        calls: std::sync::Mutex<Vec<RunCall>>,
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
                .unwrap()
                .push((program.to_string(), args.to_vec(), env.to_vec()));
            Ok(())
        }
    }

    #[tokio::test]
    async fn run_psql_file_invokes_psql_with_correct_args() {
        let runner = RecordingRunner::default();
        let target = EndpointConfig::parse("postgresql://bob:pw@tgt:5433/mydb").unwrap();
        let sql_path = std::path::Path::new("/tmp/dump.sql");
        run_psql_file(&runner, &target, sql_path, &CancellationToken::new())
            .await
            .unwrap();
        let calls = runner.calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 1);
        let (program, args, env) = &calls[0];
        assert_eq!(program, "psql");
        assert!(args.contains(&"--single-transaction".to_string()));
        assert!(args.contains(&"ON_ERROR_STOP=1".to_string()));
        assert!(args.contains(&"tgt".to_string()));
        assert!(args.contains(&"5433".to_string()));
        assert!(args.contains(&"bob".to_string()));
        assert!(args.contains(&"mydb".to_string()));
        assert!(args.contains(&"/tmp/dump.sql".to_string()));
        assert!(env.iter().any(|(k, v)| k == "PGPASSWORD" && v == "pw"));
    }

    #[tokio::test]
    async fn run_psql_file_omits_pgpassword_when_no_password() {
        let runner = RecordingRunner::default();
        let target = EndpointConfig::parse("postgresql://u@h/db").unwrap();
        run_psql_file(
            &runner,
            &target,
            std::path::Path::new("/tmp/f.sql"),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        let calls = runner.calls.lock().unwrap().clone();
        assert!(calls[0].2.is_empty());
    }

    #[tokio::test]
    async fn run_pg_restore_in_sections_cancels_mid_restore() {
        /// Runner that cancels the token on the second call (Data section).
        #[derive(Debug)]
        struct CancelOnSecond {
            cancel: CancellationToken,
            count: std::sync::Mutex<usize>,
        }

        #[async_trait]
        impl CommandRunner for CancelOnSecond {
            async fn run(
                &self,
                _program: &str,
                _args: &[String],
                _env: &[(String, String)],
                _cancel: &CancellationToken,
            ) -> Result<()> {
                let mut c = self.count.lock().unwrap();
                *c += 1;
                if *c == 2 {
                    self.cancel.cancel();
                }
                Ok(())
            }
        }

        let cancel = CancellationToken::new();
        let runner = CancelOnSecond {
            cancel: cancel.clone(),
            count: std::sync::Mutex::new(0),
        };
        let err = run_pg_restore_in_sections(&runner, &sample_request(), &cancel)
            .await
            .unwrap_err();
        assert!(matches!(err, MigrationError::Cancelled));
    }

    #[test]
    fn build_pg_restore_args_includes_jobs_for_real_directory() {
        let dir = tempfile::tempdir().unwrap();
        let mut req = sample_request();
        req.input_path = dir.path().to_path_buf();
        req.jobs = 4;
        let args = build_pg_restore_args(&req);
        assert!(args.iter().any(|a| a == "--jobs"));
        assert!(args.iter().any(|a| a == "4"));
    }

    #[tokio::test]
    async fn run_pg_restore_does_not_tolerate_non_pg_restore_failure() {
        let runner = FailingRunner {
            program_to_fail: "pg_dump".into(),
        };
        let mut req = sample_request();
        req.tolerate_errors = true;
        let err = run_pg_restore(&runner, &req, &CancellationToken::new())
            .await
            .unwrap_err();
        assert!(matches!(err, MigrationError::ExternalCommand { .. }));
    }
}
