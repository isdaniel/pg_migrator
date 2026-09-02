//! High-level migration driver.
//!
//! [`Migrator`] takes a [`MigrationConfig`] and runs the appropriate sequence
//! of dump → restore → (optional) streaming apply.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use tokio_postgres::Client;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::analyze::{maybe_analyze_target, maybe_vacuum_source};
use crate::config::{MigrationConfig, MigrationMode, VerifyMode};
use crate::copy_split::{
    export_snapshot, plan_split_tables, run_split_copy, ExportedSnapshot, SplitTable,
};
use crate::cutover::CutoverHandle;
use crate::dump::{run_pg_dump, CommandRunner, DumpFormat, DumpRequest, TokioCommandRunner};
use crate::error::{MigrationError, Result};
use crate::native_apply::{
    disable_target_subscription, drop_source_publication, drop_source_slot,
    force_clean_stale_state, run_native_apply, wait_for_slot_inactive, ApplyStats,
    PgSubscriptionLagProvider,
};
use crate::preflight::{
    ensure_publication_exists, run_offline_preflight, run_online_preflight,
    verify_publication_exists,
};
use crate::progress::{MigrationStage, ProgressEvent, ProgressReporter, TracingReporter};
use crate::restore::{run_pg_restore, RestoreRequest, RestoreSection, RESTORE_SECTIONS};
use crate::resume::{default_resume_path, CompletedStage, ResumeToken};
use crate::sequences::sync_sequences;
use crate::snapshot::prepare_replication_slot;
use crate::tls::connect_with_sslmode;
use crate::verify::verify_row_counts;

/// High-level migration driver.
#[derive(Debug)]
pub struct Migrator {
    config: MigrationConfig,
    runner: Arc<dyn CommandRunner>,
    reporter: Arc<dyn ProgressReporter>,
    /// Optional override for the dump archive path. Defaults to a `tempfile`
    /// inside `std::env::temp_dir()`.
    dump_path: Option<PathBuf>,
    /// Operator-facing handle for triggering cutover.
    cutover_handle: CutoverHandle,
}

impl Migrator {
    /// Construct a [`Migrator`] with the production defaults: the dump and
    /// restore are spawned via [`tokio::process::Command`] and progress is
    /// logged through the `tracing` subscriber.
    pub fn new(config: MigrationConfig) -> Self {
        Self {
            config,
            runner: Arc::new(TokioCommandRunner),
            reporter: Arc::new(TracingReporter),
            dump_path: None,
            cutover_handle: CutoverHandle::new(),
        }
    }

    /// Replace the [`CommandRunner`] used to invoke `pg_dump` / `pg_restore`.
    pub fn with_runner(mut self, runner: Arc<dyn CommandRunner>) -> Self {
        self.runner = runner;
        self
    }

    /// Replace the [`ProgressReporter`].
    pub fn with_reporter(mut self, reporter: Arc<dyn ProgressReporter>) -> Self {
        self.reporter = reporter;
        self
    }

    /// Pin the dump archive output path (otherwise it is generated in
    /// `std::env::temp_dir()`).
    pub fn with_dump_path(mut self, path: PathBuf) -> Self {
        self.dump_path = Some(path);
        self
    }

    /// Get a clone of the cutover handle. Hand this to a signal handler / RPC
    /// endpoint / UI so the operator can call
    /// [`CutoverHandle::request`] when ready to switch traffic to the target.
    pub fn cutover_handle(&self) -> CutoverHandle {
        self.cutover_handle.clone()
    }

    /// Get a read-only reference to the currently active configuration.
    pub fn config(&self) -> &MigrationConfig {
        &self.config
    }

    /// Run the migration pipeline.
    ///
    /// `cancel` lets the caller request a graceful shutdown — particularly
    /// important during the long-running streaming apply phase of an online
    /// migration.
    pub async fn run(&self, cancel: CancellationToken) -> Result<MigrationOutcome> {
        self.config.validate()?;

        // Standalone verify mode: a read-only count comparison. It skips the
        // offline/online preflight bundles (which create the target DB and are
        // irrelevant to a read-only compare) and is always strict — a mismatch
        // is fatal and yields a non-zero exit.
        if self.config.mode == MigrationMode::Verify {
            let report = verify_row_counts(&self.config, &cancel).await?;
            self.report(MigrationStage::Verify, report.summary_line())
                .await;
            if !report.is_ok() {
                return Err(MigrationError::config(format!(
                    "verification failed: {}",
                    report.summary_line()
                )));
            }
            return Ok(MigrationOutcome {
                stats: None,
                dump_path: std::path::PathBuf::new(),
            });
        }

        // Preflight checks open DB connections that can hang on firewall /
        // network issues. Race them against the cancel token so SIGINT is
        // honoured immediately rather than waiting for the OS connect timeout.
        let report = tokio::select! {
            _ = cancel.cancelled() => return Err(MigrationError::Cancelled),
            res = async {
                match self.config.mode {
                    MigrationMode::Offline => run_offline_preflight(&self.config).await,
                    MigrationMode::Online => run_online_preflight(&self.config).await,
                    // Verify is handled by the early return above and never
                    // reaches this match; delegate to the offline preflight to
                    // keep the match exhaustive without a panic.
                    MigrationMode::Verify => run_offline_preflight(&self.config).await,
                }
            } => res?,
        };
        self.report(MigrationStage::Validate, report.summary_line())
            .await;

        match self.config.mode {
            MigrationMode::Offline => self.run_offline(cancel).await,
            MigrationMode::Online => self.run_online(cancel).await,
            // Verify is handled by the early return above and never reaches
            // this match; delegate to the offline path to keep the match
            // exhaustive without a panic.
            MigrationMode::Verify => self.run_offline(cancel).await,
        }
    }

    /// Offline path: `pg_dump` → `pg_restore`.
    async fn run_offline(&self, cancel: CancellationToken) -> Result<MigrationOutcome> {
        let dump_path = self.dump_path_or_default("dump_offline");
        let mut token = self.load_or_init_resume(&dump_path).await?;

        self.run_source_vacuum_stage(&mut token, &dump_path).await;

        // Offline has no replication slot, so if the split copy is enabled we
        // export our own snapshot and hand the same id to pg_dump. Held for
        // the whole run: the directly-copied tables must agree with the
        // archived ones about which rows exist.
        let split = self.prepare_split(None).await?;

        if !token.has(CompletedStage::Dump) {
            self.report(MigrationStage::Dump, "starting pg_dump").await;
            let started = Instant::now();
            run_pg_dump(
                self.runner.as_ref(),
                &self.dump_request(
                    &dump_path,
                    split.as_ref().map(|s| s.snapshot_id.clone()),
                    split.as_ref(),
                ),
                &cancel,
            )
            .await?;
            if cancel.is_cancelled() {
                return Err(MigrationError::Cancelled);
            }
            self.report_dump_elapsed(started).await;
            token.mark(CompletedStage::Dump);
            self.save_resume(&token, &dump_path).await;
        } else {
            self.report(
                MigrationStage::Dump,
                "skipped (resume): pg_dump already complete",
            )
            .await;
        }

        if !token.has(CompletedStage::Restore) {
            self.report(MigrationStage::Restore, "starting pg_restore")
                .await;
            self.restore(&dump_path, &cancel, split.as_ref()).await?;
            token.mark(CompletedStage::Restore);
            self.save_resume(&token, &dump_path).await;
        } else {
            self.report(
                MigrationStage::Restore,
                "skipped (resume): pg_restore already complete",
            )
            .await;
        }

        self.run_target_analyze_stage(&mut token, &dump_path).await;

        self.run_verify_stage(&cancel).await?;

        self.report(MigrationStage::Complete, "offline migration finished")
            .await;
        Ok(MigrationOutcome {
            stats: None,
            dump_path,
        })
    }

    /// Online path: slot + snapshot → snapshot-aligned dump → restore →
    /// streaming apply.
    async fn run_online(&self, cancel: CancellationToken) -> Result<MigrationOutcome> {
        // 0. Optional best-effort cleanup of leftovers from a previous run.
        if self.config.online.force_clean {
            self.report(
                MigrationStage::Validate,
                "force-clean: dropping any stale subscription/slot",
            )
            .await;
            force_clean_stale_state(
                &self.config.source.connection_string,
                &self.config.target.connection_string,
                &self.config.online,
            )
            .await?;
        }

        // Pre-dump: VACUUM ANALYZE on source.
        let dump_path = self.dump_path_or_default("dump_online");
        let mut token = self.load_or_init_resume(&dump_path).await?;

        self.run_source_vacuum_stage(&mut token, &dump_path).await;

        // When resuming past Dump, the slot was created in a previous run
        // and the exported snapshot is already gone — there is no live
        // stream to keep. We only call `prepare_replication_slot` (and
        // hold a stream) when we still need to run pg_dump.
        let mut prepared_stream = None;
        let snapshot_name = if !token.has(CompletedStage::Dump) {
            // Ensure the publication exists on the source. If auto-create
            // is enabled and it's missing, create it. Otherwise fail fast
            // so the apply worker doesn't error 10+ minutes later.
            if self.config.online.auto_create_publication {
                self.report(
                    MigrationStage::Validate,
                    format!(
                        "ensuring publication `{}` exists on source (auto-create enabled)",
                        self.config.online.publication
                    ),
                )
                .await;
                let created = ensure_publication_exists(
                    &self.config.source.connection_string,
                    &self.config.online.publication,
                    &self.config.tables,
                    &self.config.schemas,
                    &self.config.exclude_tables,
                    &self.config.exclude_schemas,
                )
                .await?;
                if created {
                    token.pub_auto_created = true;
                    self.save_resume(&token, &dump_path).await;
                }
            } else {
                self.report(
                    MigrationStage::Validate,
                    format!(
                        "verifying publication `{}` exists on source",
                        self.config.online.publication
                    ),
                )
                .await;
                verify_publication_exists(
                    &self.config.source.connection_string,
                    &self.config.online.publication,
                )
                .await?;
            }

            // 1. Prepare slot + snapshot (must happen *before* pg_dump runs).
            self.report(MigrationStage::PrepareSnapshot, "creating replication slot")
                .await;
            let prepared = prepare_replication_slot(
                &self.config.source.connection_string,
                &self.config.online,
            )
            .await?;
            let snap = prepared.snapshot_name.clone();
            prepared_stream = Some(prepared.stream);
            token.mark(CompletedStage::PrepareSnapshot);
            token.snapshot_name = snap.clone();
            self.save_resume(&token, &dump_path).await;
            snap
        } else {
            self.report(
                MigrationStage::PrepareSnapshot,
                "skipped (resume): slot/snapshot already prepared in previous run",
            )
            .await;
            token.snapshot_name.clone()
        };

        // 2. Snapshot-aligned dump.
        // The slot already exported a snapshot, so the split copy reuses it
        // rather than exporting a second one.
        let split = self.prepare_split(snapshot_name.clone()).await?;

        if !token.has(CompletedStage::Dump) {
            self.report(
                MigrationStage::Dump,
                format!(
                    "starting pg_dump with snapshot {}",
                    snapshot_name.as_deref().unwrap_or("<unknown>")
                ),
            )
            .await;
            let started = Instant::now();
            run_pg_dump(
                self.runner.as_ref(),
                &self.dump_request(&dump_path, snapshot_name.clone(), split.as_ref()),
                &cancel,
            )
            .await?;
            if cancel.is_cancelled() {
                return Err(MigrationError::Cancelled);
            }
            self.report_dump_elapsed(started).await;
            token.mark(CompletedStage::Dump);
            self.save_resume(&token, &dump_path).await;
        } else {
            self.report(
                MigrationStage::Dump,
                "skipped (resume): pg_dump already complete",
            )
            .await;
        }

        // 3. Restore.
        if !token.has(CompletedStage::Restore) {
            self.report(MigrationStage::Restore, "starting pg_restore")
                .await;
            self.restore(&dump_path, &cancel, split.as_ref()).await?;
            if cancel.is_cancelled() {
                return Err(MigrationError::Cancelled);
            }
            token.mark(CompletedStage::Restore);
            self.save_resume(&token, &dump_path).await;
        } else {
            self.report(
                MigrationStage::Restore,
                "skipped (resume): pg_restore already complete",
            )
            .await;
        }

        // 3.5. Post-restore: ANALYZE on target.
        self.run_target_analyze_stage(&mut token, &dump_path).await;

        // 4. Streaming apply via `CREATE SUBSCRIPTION` on the target. The
        // pg_walstream stream's only job was to keep the exported snapshot
        // alive across pg_dump; the slot itself persists on the source
        // independently of the stream connection, so we drop the stream
        // before handing the slot to the native apply worker.
        drop(prepared_stream);

        // When resuming into the apply phase a previous (crashed) run may
        // already have created the subscription. We leave it in place so
        // run_native_apply can re-enable it (preserving the replication
        // origin). Dropping it would lose origin tracking and cause
        // duplicate key violations when the new subscription replays WAL.
        // However, we must disable it first so the old apply worker
        // releases the slot before we re-enable with a fresh connection.
        if self.config.resume {
            disable_target_subscription(&self.config.target.connection_string, &self.config.online)
                .await;
        }

        // Wait for the slot to become inactive. On resume the old apply
        // worker may still hold the walsender connection briefly after
        // being disabled; on first run the slot should already be free.
        wait_for_slot_inactive(
            &self.config.source.connection_string,
            &self.config.online.slot_name,
            self.reporter.as_ref(),
        )
        .await?;

        // 4.5. Verify pglogical is NOT interfering with native logical replication.
        // (handled by the online preflight bundle in Migrator::run)

        let stats = self.run_native_engine(cancel.clone()).await?;
        token.last_applied_lsn = Some(stats.last_applied_lsn);
        self.save_resume(&token, &dump_path).await;

        // After a cutover-driven exit, sync sequences so the target's
        // `last_value`s match the source. PostgreSQL logical replication
        // does NOT replay nextval(), so without this step the first
        // post-cutover INSERT … DEFAULT nextval(...) would collide with
        // a row already replicated by the apply worker.
        if stats.cutover_triggered && self.config.online.sync_sequences_on_cutover {
            self.report(
                MigrationStage::Cutover,
                "syncing sequences from source to target",
            )
            .await;
            match sync_sequences(
                &self.config.source.connection_string,
                &self.config.target.connection_string,
                &self.config.schemas,
            )
            .await
            {
                Ok(applied) => {
                    self.report(
                        MigrationStage::Cutover,
                        format!("synced {applied} sequence(s) from source to target"),
                    )
                    .await;
                }
                Err(e) => {
                    // Sequence sync is best-effort — if a managed-PG
                    // role can't write to one of the target sequences,
                    // we should not roll back the otherwise-successful
                    // cutover. Surface a loud warning so the operator
                    // can fix it manually before re-pointing traffic.
                    tracing::warn!(
                        error = %e,
                        "sequence sync failed — manually run \
                         `SELECT setval('<seq>', <value>, true)` on the target \
                         for each sequence before resuming application traffic",
                    );
                    self.report(
                        MigrationStage::Cutover,
                        format!(
                            "sequence sync failed: {e} (manual sync required \
                             before re-enabling traffic)"
                        ),
                    )
                    .await;
                }
            }
        }

        // NOTE: online mode does NOT run an automatic row-count verification.
        // Cutover (Ctrl+C) stops the apply worker but does not freeze source
        // writes, so an automatic count(*) compare would show spurious
        // mismatches. Readiness is signalled by the lag heartbeat, not row
        // counts. After the operator quiesces source writes and lag has
        // drained, run verification manually via `--mode verify`.

        // 6. Post-cutover cleanup: drop auto-created publication and slot.
        if stats.cutover_triggered {
            cleanup_source_after_cutover(
                &self.config.source.connection_string,
                &self.config.online,
                token.pub_auto_created,
                self.reporter.as_ref(),
            )
            .await;
        }

        self.report(MigrationStage::Complete, "online migration finished")
            .await;
        Ok(MigrationOutcome {
            stats: Some(stats),
            dump_path,
        })
    }

    /// Native PostgreSQL logical-replication apply path
    /// (`CREATE SUBSCRIPTION` on target).
    async fn run_native_engine(&self, cancel: CancellationToken) -> Result<ApplyStats> {
        let target_client = self.connect_target().await?;
        self.report(
            MigrationStage::StreamApply,
            "starting native logical-replication apply (CREATE SUBSCRIPTION)",
        )
        .await;

        let lag_provider = PgSubscriptionLagProvider::connect(
            &self.config.source.connection_string,
            &self.config.online.slot_name,
        )
        .await?;

        // The CONNECTION clause inside CREATE SUBSCRIPTION is dialed by the
        // target's apply worker, not by us — its network view of the source
        // may not match ours (e.g. operator on host vs. target in container).
        let subscription_source = self
            .config
            .online
            .subscription_source_conn
            .as_deref()
            .unwrap_or(&self.config.source.connection_string);

        run_native_apply(
            &target_client,
            &lag_provider,
            &self.config.online,
            subscription_source,
            self.cutover_handle.clone(),
            self.reporter.as_ref(),
            cancel,
        )
        .await
    }

    /// Resolve the split-copy plan for this run, or `None` when the feature
    /// is off.
    ///
    /// `existing_snapshot` is the id already exported by the replication
    /// slot (online mode). Offline mode passes `None` and gets a freshly
    /// exported one, held open by the returned context.
    async fn prepare_split(
        &self,
        existing_snapshot: Option<String>,
    ) -> Result<Option<SplitContext>> {
        let Some(threshold) = self.config.split_tables_larger_than else {
            return Ok(None);
        };

        let plan = plan_split_tables(
            &self.config.source.connection_string,
            threshold,
            self.config.split_max_parts,
        )
        .await?;
        if plan.is_empty() {
            self.report(
                MigrationStage::Dump,
                format!(
                    "no table reaches the {threshold}-byte split threshold; \
                         using the normal dump path"
                ),
            )
            .await;
            return Ok(None);
        }

        // Online mode reuses the slot's snapshot. If the slot was reused and
        // PostgreSQL exported nothing, there is no consistent point to copy
        // from — refuse rather than silently copy from a different instant
        // than the archive.
        let (snapshot_id, held) = match existing_snapshot {
            Some(id) => (id, None),
            None if self.config.mode == MigrationMode::Online => {
                return Err(MigrationError::config(
                    "--split-tables-larger-than needs an exported snapshot, but the \
                     replication slot did not provide one (it was reused from a \
                     previous run). Drop the slot, or run without the split option.",
                ))
            }
            None => {
                let held = export_snapshot(&self.config.source.connection_string).await?;
                (held.id.clone(), Some(held))
            }
        };

        let total: i64 = plan.iter().map(|t| t.bytes).sum();
        self.report_detail(
            MigrationStage::Dump,
            format!(
                "{} table(s) will be copied directly, bypassing the archive",
                plan.len()
            ),
            serde_json::json!({
                "tables": plan.iter().map(|t| t.table.as_str()).collect::<Vec<_>>(),
                "bytes": total,
                "parts_per_table": self.config.split_max_parts,
            }),
        )
        .await;

        Ok(Some(SplitContext {
            plan,
            snapshot_id,
            _held: held,
        }))
    }

    fn dump_request(
        &self,
        dump_path: &Path,
        snapshot: Option<String>,
        split: Option<&SplitContext>,
    ) -> DumpRequest {
        // Custom format dump → fastest pg_restore; directory format if user
        // has asked for >1 jobs. We default to Custom to avoid surprising the
        // operator with a directory archive.
        let format = if self.config.jobs > 1 {
            DumpFormat::Directory
        } else {
            DumpFormat::Custom
        };
        DumpRequest {
            source: self.config.source.clone(),
            scope: self.config.dump_scope,
            jobs: self.config.jobs,
            snapshot,
            schemas: self.config.schemas.clone(),
            tables: self.config.tables.clone(),
            exclude_schemas: self.config.exclude_schemas.clone(),
            exclude_tables: self.config.exclude_tables.clone(),
            exclude_table_data: split
                .map(|s| s.plan.iter().map(|t| t.table.clone()).collect())
                .unwrap_or_default(),
            output_path: dump_path.to_path_buf(),
            format,
            no_publications: self.config.no_publications,
            no_subscriptions: self.config.no_subscriptions,
            compress: self.config.dump_compress.clone(),
            no_sync: self.config.no_sync,
            no_comments: self.config.no_comments,
            no_security_labels: self.config.no_security_labels,
            no_table_access_method: self.config.no_table_access_method,
        }
    }

    fn restore_request(&self, dump_path: &Path) -> RestoreRequest {
        RestoreRequest {
            target: self.config.target.clone(),
            input_path: dump_path.to_path_buf(),
            jobs: self.config.jobs,
            clean: self.config.drop_target_first,
            no_owner: true,
            no_acl: true,
            tolerate_errors: self.config.allow_restore_errors,
            section: None,
        }
    }

    /// Issue `pg_restore` either as a single all-in-one call or, when
    /// `split_sections` is enabled, as three section-restricted calls
    /// (pre-data → data → post-data).
    ///
    /// The split path reports each section separately. On a large table the
    /// data phase and the post-data phase (index builds) can differ by an
    /// order of magnitude, and an operator deciding where to spend tuning
    /// effort cannot see that from a single aggregate `Restore` event.
    async fn restore(
        &self,
        dump_path: &Path,
        cancel: &CancellationToken,
        split: Option<&SplitContext>,
    ) -> Result<()> {
        let base = self.restore_request(dump_path);
        if !self.config.split_sections {
            run_pg_restore(self.runner.as_ref(), &base, cancel).await?;
            return self.run_split_copy_stage(split, cancel).await;
        }
        for section in RESTORE_SECTIONS {
            if cancel.is_cancelled() {
                return Err(MigrationError::Cancelled);
            }
            let mut req = base.clone();
            req.section = Some(section);
            // Keep emitting the same line `run_pg_restore_in_sections` did:
            // it is the section-ordering contract the integration suite
            // asserts on, and moving the loop here must not change it.
            info!(?section, "running pg_restore section");
            let started = Instant::now();
            run_pg_restore(self.runner.as_ref(), &req, cancel).await?;
            self.report_detail(
                MigrationStage::Restore,
                format!("section {} complete", section.flag()),
                serde_json::json!({
                    "section": section.flag(),
                    "elapsed_secs": started.elapsed().as_secs_f64(),
                }),
            )
            .await;
            // The split tables' rows are not in the archive. Move them once
            // the archive's own data is in and before post-data starts
            // building indexes over them.
            if section == RestoreSection::Data {
                self.run_split_copy_stage(split, cancel).await?;
            }
        }
        Ok(())
    }

    /// Stream the split tables straight from source to target. No-op when
    /// the feature is off.
    async fn run_split_copy_stage(
        &self,
        split: Option<&SplitContext>,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let Some(split) = split else {
            return Ok(());
        };
        let started = Instant::now();
        let rows = run_split_copy(
            &self.config.source.connection_string,
            &self.config.target.connection_string,
            &split.snapshot_id,
            &split.plan,
            self.config.drop_target_first,
            cancel,
        )
        .await?;
        self.report_detail(
            MigrationStage::Restore,
            format!("split copy complete ({rows} rows)"),
            serde_json::json!({
                "section": "split-copy",
                "rows": rows,
                "tables": split.plan.len(),
                "elapsed_secs": started.elapsed().as_secs_f64(),
            }),
        )
        .await;
        Ok(())
    }

    fn dump_path_or_default(&self, prefix: &str) -> PathBuf {
        if let Some(p) = &self.dump_path {
            return p.clone();
        }
        if let Some(p) = &self.config.dump_path {
            return p.clone();
        }
        let mut p = std::env::temp_dir();
        p.push(format!("{prefix}-{}", std::process::id()));
        p
    }

    fn resume_path(&self, dump_path: &Path) -> PathBuf {
        self.config
            .resume_file
            .clone()
            .unwrap_or_else(|| default_resume_path(dump_path))
    }

    /// Load (or freshly create) the resume token used to skip already-
    /// completed stages. When `--resume` is off, returns a brand-new
    /// in-memory token that is also persisted on every successful stage
    /// so a future run *can* resume even if the operator forgot to opt
    /// in this time. The path is honoured strictly only when
    /// `config.resume == true`.
    async fn load_or_init_resume(&self, dump_path: &Path) -> Result<ResumeToken> {
        let path = self.resume_path(dump_path);
        if self.config.resume {
            match ResumeToken::load(&path).await? {
                Some(token) => {
                    token.check_compatible(&self.config)?;
                    info!(
                        path = %path.display(),
                        completed = ?token.completed,
                        "resume token loaded — skipping completed stages"
                    );
                    Ok(token)
                }
                None => {
                    info!(
                        path = %path.display(),
                        "--resume set but no token on disk; running from scratch"
                    );
                    Ok(ResumeToken::new(&self.config, dump_path.to_path_buf()))
                }
            }
        } else {
            Ok(ResumeToken::new(&self.config, dump_path.to_path_buf()))
        }
    }

    async fn save_resume(&self, token: &ResumeToken, dump_path: &Path) {
        let path = self.resume_path(dump_path);
        if let Err(e) = token.save(&path).await {
            // Resume is a best-effort accelerator — never abort the
            // real migration because we couldn't write the token.
            tracing::warn!(error = %e, path = %path.display(), "failed to save resume token");
        }
    }

    async fn connect_target(&self) -> Result<Client> {
        info!("connecting to target {}", self.config.target.redacted());
        connect_with_sslmode(&self.config.target.connection_string).await
    }

    /// Run the pre-dump VACUUM ANALYZE on source if enabled and not already
    /// completed. Non-fatal: logs a warning on failure and continues.
    async fn run_source_vacuum_stage(&self, token: &mut ResumeToken, dump_path: &std::path::Path) {
        if self.config.skip_source_vacuum || token.has(CompletedStage::SourceVacuum) {
            return;
        }
        self.report(
            MigrationStage::SourceVacuum,
            "running VACUUM ANALYZE on source",
        )
        .await;
        match maybe_vacuum_source(&self.config).await {
            Ok(()) => {
                token.mark(CompletedStage::SourceVacuum);
                self.save_resume(token, dump_path).await;
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "VACUUM ANALYZE on source failed (non-fatal, continuing)"
                );
            }
        }
    }

    /// Run post-restore ANALYZE on target if enabled and not already
    /// completed. Non-fatal: logs a warning on failure and continues.
    async fn run_target_analyze_stage(&self, token: &mut ResumeToken, dump_path: &std::path::Path) {
        if self.config.skip_analyze || token.has(CompletedStage::Analyze) {
            return;
        }
        self.report(MigrationStage::Analyze, "running ANALYZE on target")
            .await;
        match maybe_analyze_target(&self.config).await {
            Ok(()) => {
                token.mark(CompletedStage::Analyze);
                self.save_resume(token, dump_path).await;
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "ANALYZE on target failed (non-fatal, continuing)"
                );
            }
        }
    }

    /// Run the row-count verification step. Used by OFFLINE mode (after
    /// restore) and the standalone `--mode verify` command; ONLINE mode does
    /// not auto-verify (see the note in [`Self::run_online`]). Reports a
    /// summary. On mismatch: warns when `verify` is `Warn` (default) or returns
    /// an error when it is `Strict`. Skipped entirely when `verify` is `Off`.
    /// Threads `cancel` through to [`verify_row_counts`] so a long verify can be
    /// aborted.
    async fn run_verify_stage(&self, cancel: &CancellationToken) -> Result<()> {
        if self.config.verify == VerifyMode::Off {
            self.report(MigrationStage::Verify, "skipped (--verify off)")
                .await;
            return Ok(());
        }
        let report = verify_row_counts(&self.config, cancel).await?;
        self.report(MigrationStage::Verify, report.summary_line())
            .await;
        if !report.is_ok() && self.config.verify == VerifyMode::Strict {
            return Err(MigrationError::config(format!(
                "verification failed: {} — see warnings above",
                report.summary_line()
            )));
        }
        Ok(())
    }

    async fn report(&self, stage: MigrationStage, message: impl Into<String>) {
        self.reporter
            .report(ProgressEvent::new(stage, message.into()))
            .await;
    }

    async fn report_detail(
        &self,
        stage: MigrationStage,
        message: impl Into<String>,
        detail: serde_json::Value,
    ) {
        self.reporter
            .report(ProgressEvent::new(stage, message.into()).with_detail(detail))
            .await;
    }

    /// Emit the wall time of the dump so it can be compared against the
    /// per-section restore timings from [`Self::restore`]. Together they
    /// answer "is this migration bound by moving data or by rebuilding
    /// indexes", which is the first question when tuning a large run.
    async fn report_dump_elapsed(&self, started: Instant) {
        self.report_detail(
            MigrationStage::Dump,
            "pg_dump complete",
            serde_json::json!({ "elapsed_secs": started.elapsed().as_secs_f64() }),
        )
        .await;
    }
}

/// Everything the split-copy path needs, resolved once per run.
///
/// Constructed by `Migrator::prepare_split` and threaded through the dump
/// (which must exclude these tables' data) and the restore (which must
/// supply it directly instead).
#[allow(missing_debug_implementations)]
struct SplitContext {
    /// Tables selected for direct copying, with their page ranges.
    plan: Vec<SplitTable>,
    /// Snapshot every stream reads from, shared with `pg_dump`.
    snapshot_id: String,
    /// Offline runs hold their own exported snapshot here; dropping this
    /// ends the exporting transaction, so it must outlive the copy. Online
    /// runs leave it `None` — the replication slot owns the snapshot.
    _held: Option<ExportedSnapshot>,
}

/// Aggregate result of a single migration run.
#[derive(Debug, Clone)]
pub struct MigrationOutcome {
    /// Streaming apply statistics (only present for online migrations).
    pub stats: Option<ApplyStats>,
    /// Final dump archive path (kept on disk for inspection / re-runs).
    pub dump_path: PathBuf,
}

impl MigrationOutcome {
    /// Whether the online apply loop ended because cutover was triggered
    /// (operator-driven or auto). Always `false` for offline migrations.
    pub fn cutover_triggered(&self) -> bool {
        self.stats.map(|s| s.cutover_triggered).unwrap_or(false)
    }
}

/// Post-cutover cleanup: drop auto-created publication and replication slot
/// on the source. Non-fatal — errors are logged as warnings.
pub async fn cleanup_source_after_cutover(
    source_conn: &str,
    online: &crate::config::OnlineOptions,
    pub_auto_created: bool,
    reporter: &dyn ProgressReporter,
) {
    if pub_auto_created {
        reporter
            .report(ProgressEvent::new(
                MigrationStage::SourceCleanup,
                format!(
                    "dropping auto-created publication `{}` on source",
                    online.publication
                ),
            ))
            .await;
        if let Err(e) = drop_source_publication(source_conn, &online.publication).await {
            tracing::warn!(
                error = %e,
                "failed to drop auto-created publication (non-fatal)"
            );
        }
    }

    if online.drop_slot_on_cutover {
        reporter
            .report(ProgressEvent::new(
                MigrationStage::SourceCleanup,
                format!("dropping replication slot `{}` on source", online.slot_name),
            ))
            .await;
        if let Err(e) = wait_for_slot_inactive(source_conn, &online.slot_name, reporter).await {
            tracing::warn!(
                error = %e,
                "failed waiting for slot to become inactive (non-fatal)"
            );
        }
        if let Err(e) = drop_source_slot(source_conn, &online.slot_name).await {
            tracing::warn!(
                error = %e,
                "failed to drop replication slot (non-fatal)"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{EndpointConfig, OnlineOptions};
    use crate::dump::{CommandRunner, DumpFormat};
    use crate::progress::CollectingReporter;
    use async_trait::async_trait;
    use std::sync::Mutex;

    /// Records every command dispatched without spawning real processes.
    #[derive(Debug, Default)]
    struct RecordingRunner {
        calls: Mutex<Vec<(String, Vec<String>)>>,
    }

    impl RecordingRunner {
        fn snapshot(&self) -> Vec<(String, Vec<String>)> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl CommandRunner for RecordingRunner {
        async fn run(
            &self,
            program: &str,
            args: &[String],
            _env: &[(String, String)],
            _cancel: &CancellationToken,
        ) -> Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push((program.to_string(), args.to_vec()));
            Ok(())
        }
    }

    fn baseline_config() -> MigrationConfig {
        MigrationConfig {
            source: EndpointConfig::parse("postgres://u:p@src/db").unwrap(),
            target: EndpointConfig::parse("postgres://u:p@dst/db").unwrap(),
            skip_analyze: true,
            skip_source_vacuum: true,
            verify: VerifyMode::Off,
            ..MigrationConfig::default()
        }
    }

    #[tokio::test]
    async fn offline_run_invokes_dump_then_restore() {
        let runner = Arc::new(RecordingRunner::default());
        let reporter = Arc::new(CollectingReporter::new());
        let migrator = Migrator::new(MigrationConfig {
            split_sections: false,
            ..baseline_config()
        })
        .with_runner(runner.clone())
        .with_reporter(reporter.clone())
        .with_dump_path(PathBuf::from("/tmp/pg_dbmigrator_test_dump"));

        migrator
            .run_offline(CancellationToken::new())
            .await
            .expect("offline migration should succeed");

        let calls = runner.snapshot();
        assert_eq!(calls.len(), 2, "expected 2 calls (dump+restore)");
        assert_eq!(calls[0].0, "pg_dump");
        assert_eq!(calls[1].0, "pg_restore");

        let stages: Vec<_> = reporter
            .events()
            .await
            .into_iter()
            .map(|e| e.stage)
            .collect();
        assert!(stages.contains(&MigrationStage::Dump));
        assert!(stages.contains(&MigrationStage::Restore));
        assert!(stages.contains(&MigrationStage::Complete));
    }

    /// The point of the per-section events is that an operator can read
    /// "data took X, post-data took Y" out of the `--json` stream. Assert
    /// the payload shape, not just that some event was emitted.
    #[tokio::test]
    async fn split_section_restore_reports_elapsed_per_section() {
        let reporter = Arc::new(CollectingReporter::new());
        let migrator = Migrator::new(MigrationConfig {
            split_sections: true,
            ..baseline_config()
        })
        .with_runner(Arc::new(RecordingRunner::default()))
        .with_reporter(reporter.clone())
        .with_dump_path(PathBuf::from("/tmp/pg_dbmigrator_test_sections"));

        migrator
            .run_offline(CancellationToken::new())
            .await
            .expect("offline migration should succeed");

        let sections: Vec<String> = reporter
            .events()
            .await
            .into_iter()
            .filter(|e| e.stage == MigrationStage::Restore)
            .filter_map(|e| e.detail)
            .map(|d| {
                assert!(
                    d["elapsed_secs"].is_number(),
                    "section event must carry elapsed_secs: {d}"
                );
                d["section"].as_str().unwrap().to_string()
            })
            .collect();
        assert_eq!(sections, ["pre-data", "data", "post-data"]);
    }

    #[tokio::test]
    async fn offline_run_with_split_sections_invokes_pg_restore_three_times() {
        let runner = Arc::new(RecordingRunner::default());
        let reporter = Arc::new(CollectingReporter::new());
        let cfg = MigrationConfig {
            split_sections: true,
            ..baseline_config()
        };
        let migrator = Migrator::new(cfg)
            .with_runner(runner.clone())
            .with_reporter(reporter)
            .with_dump_path(PathBuf::from("/tmp/pg_dbmigrator_split_dump"));

        migrator
            .run_offline(CancellationToken::new())
            .await
            .expect("split-section restore should succeed");

        let calls = runner.snapshot();
        assert_eq!(calls.len(), 4, "1 dump + 3 restore expected");
        assert_eq!(calls[0].0, "pg_dump");
        let sections: Vec<_> = calls[1..]
            .iter()
            .map(|(prog, args)| {
                assert_eq!(prog, "pg_restore");
                args.iter()
                    .find(|a| a.starts_with("--section="))
                    .cloned()
                    .unwrap_or_default()
            })
            .collect();
        assert_eq!(
            sections,
            vec![
                "--section=pre-data".to_string(),
                "--section=data".to_string(),
                "--section=post-data".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn validation_failure_short_circuits() {
        let cfg = MigrationConfig {
            jobs: 0,
            ..baseline_config()
        };
        let migrator = Migrator::new(cfg);
        let err = migrator.run(CancellationToken::new()).await.unwrap_err();
        assert!(matches!(err, MigrationError::Config(_)));
    }

    #[tokio::test]
    async fn offline_run_skips_dump_when_resume_token_says_dump_complete() {
        let dir = tempfile::tempdir().unwrap();
        let dump = dir.path().join("dump");
        let resume = dir.path().join("dump.resume.json");

        let cfg = MigrationConfig {
            resume: true,
            dump_path: Some(dump.clone()),
            resume_file: Some(resume.clone()),
            split_sections: false,
            ..baseline_config()
        };

        // Pre-seed the token: Dump already complete, Restore not yet.
        let mut t = crate::resume::ResumeToken::new(&cfg, dump.clone());
        t.mark(crate::resume::CompletedStage::Dump);
        t.save(&resume).await.unwrap();

        let runner = Arc::new(RecordingRunner::default());
        let migrator = Migrator::new(cfg)
            .with_runner(runner.clone())
            .with_reporter(Arc::new(CollectingReporter::new()));

        migrator
            .run_offline(CancellationToken::new())
            .await
            .unwrap();

        let calls = runner.snapshot();
        assert_eq!(calls.len(), 1, "expected 1 call (restore only)");
        assert_eq!(calls[0].0, "pg_restore");
    }

    #[tokio::test]
    async fn validation_rejects_resume_without_dump_path() {
        let cfg = MigrationConfig {
            resume: true,
            dump_path: None,
            ..baseline_config()
        };
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, MigrationError::Config(_)));
    }

    #[test]
    fn dump_request_uses_directory_format_for_parallel_jobs() {
        let cfg = MigrationConfig {
            jobs: 4,
            ..baseline_config()
        };
        let m = Migrator::new(cfg);
        let req = m.dump_request(Path::new("/tmp/dump"), None, None);
        assert_eq!(req.format, DumpFormat::Directory);
    }

    #[test]
    fn dump_request_uses_custom_format_for_single_job() {
        let cfg = MigrationConfig {
            jobs: 1,
            ..baseline_config()
        };
        let m = Migrator::new(cfg);
        let req = m.dump_request(Path::new("/tmp/dump"), None, None);
        assert_eq!(req.format, DumpFormat::Custom);
    }

    #[test]
    fn dump_request_propagates_perf_flags() {
        let cfg = MigrationConfig {
            dump_compress: Some("zstd:3".into()),
            no_sync: true,
            no_comments: true,
            no_security_labels: true,
            no_table_access_method: true,
            ..baseline_config()
        };
        let m = Migrator::new(cfg);
        let req = m.dump_request(Path::new("/tmp/dump"), None, None);
        assert_eq!(req.compress.as_deref(), Some("zstd:3"));
        assert!(req.no_sync);
        assert!(req.no_comments);
        assert!(req.no_security_labels);
        assert!(req.no_table_access_method);
    }

    #[test]
    fn online_validation_inherits_offline_checks() {
        let cfg = MigrationConfig {
            mode: MigrationMode::Online,
            online: OnlineOptions {
                slot_name: "".into(),
                ..OnlineOptions::default()
            },
            ..baseline_config()
        };
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, MigrationError::Config(_)));
    }

    #[test]
    fn cutover_handle_is_clonable_and_stable_across_calls() {
        let m = Migrator::new(baseline_config());
        let h1 = m.cutover_handle();
        let h2 = m.cutover_handle();
        assert!(!h1.is_requested());
        h1.request();
        // Both clones share state with the migrator's internal handle.
        assert!(h2.is_requested());
    }

    #[test]
    fn migration_outcome_cutover_triggered_reflects_stats() {
        let mut out = MigrationOutcome {
            stats: None,
            dump_path: PathBuf::from("/tmp/x"),
        };
        assert!(!out.cutover_triggered()); // offline: always false

        out.stats = Some(ApplyStats {
            cutover_triggered: true,
            ..ApplyStats::default()
        });
        assert!(out.cutover_triggered());
    }

    #[test]
    fn config_accessor_returns_migrator_config() {
        let cfg = baseline_config();
        let m = Migrator::new(cfg.clone());
        assert_eq!(m.config().source.host, "src");
        assert_eq!(m.config().target.host, "dst");
        assert!(matches!(m.config().mode, MigrationMode::Offline));
    }

    #[tokio::test]
    async fn offline_run_cancels_after_dump_before_restore() {
        use std::sync::atomic::{AtomicBool, Ordering};

        #[derive(Debug)]
        struct CancellingRunner {
            cancel: CancellationToken,
            called: AtomicBool,
        }

        #[async_trait]
        impl CommandRunner for CancellingRunner {
            async fn run(
                &self,
                program: &str,
                _args: &[String],
                _env: &[(String, String)],
                _cancel: &CancellationToken,
            ) -> Result<()> {
                if program == "pg_dump" {
                    self.cancel.cancel();
                }
                self.called.store(true, Ordering::SeqCst);
                Ok(())
            }
        }

        let cancel = CancellationToken::new();
        let runner = Arc::new(CancellingRunner {
            cancel: cancel.clone(),
            called: AtomicBool::new(false),
        });
        let reporter = Arc::new(CollectingReporter::new());
        let temp_dir = tempfile::tempdir().unwrap();
        let migrator = Migrator::new(baseline_config())
            .with_runner(runner)
            .with_reporter(reporter)
            .with_dump_path(temp_dir.path().join("cancel_test"));

        let err = migrator.run_offline(cancel).await.unwrap_err();
        assert!(matches!(err, MigrationError::Cancelled));
    }

    #[tokio::test]
    async fn offline_run_skips_both_dump_and_restore_when_resume_complete() {
        let dir = tempfile::tempdir().unwrap();
        let dump = dir.path().join("dump");
        let resume = dir.path().join("dump.resume.json");

        let cfg = MigrationConfig {
            resume: true,
            dump_path: Some(dump.clone()),
            resume_file: Some(resume.clone()),
            split_sections: false,
            ..baseline_config()
        };

        let mut t = crate::resume::ResumeToken::new(&cfg, dump.clone());
        t.mark(crate::resume::CompletedStage::Dump);
        t.mark(crate::resume::CompletedStage::Restore);
        t.save(&resume).await.unwrap();

        let runner = Arc::new(RecordingRunner::default());
        let migrator = Migrator::new(cfg)
            .with_runner(runner.clone())
            .with_reporter(Arc::new(CollectingReporter::new()));

        migrator
            .run_offline(CancellationToken::new())
            .await
            .unwrap();
        assert!(
            runner.snapshot().is_empty(),
            "neither dump nor restore should have been invoked"
        );
    }

    #[test]
    fn dump_request_propagates_exclude_schemas_and_tables() {
        let cfg = MigrationConfig {
            exclude_schemas: vec!["audit".into(), "temp".into()],
            exclude_tables: vec!["public.large".into()],
            ..baseline_config()
        };
        let m = Migrator::new(cfg);
        let req = m.dump_request(Path::new("/tmp/dump"), None, None);
        assert_eq!(req.exclude_schemas, vec!["audit", "temp"]);
        assert_eq!(req.exclude_tables, vec!["public.large"]);
    }

    #[test]
    fn dump_request_propagates_snapshot() {
        let cfg = baseline_config();
        let m = Migrator::new(cfg);
        let req = m.dump_request(
            Path::new("/tmp/dump"),
            Some("00000003-deadbeef-1".into()),
            None,
        );
        assert_eq!(req.snapshot.as_deref(), Some("00000003-deadbeef-1"));
    }

    #[test]
    fn with_dump_path_pins_dump_archive_location() {
        let cfg = baseline_config();
        let m = Migrator::new(cfg).with_dump_path(PathBuf::from("/custom/dump"));
        let path = m.dump_path_or_default("test_prefix");
        assert_eq!(path, PathBuf::from("/custom/dump"));
    }

    #[test]
    fn dump_path_or_default_uses_config_dump_path_when_no_override() {
        let cfg = MigrationConfig {
            dump_path: Some(PathBuf::from("/config/level/dump")),
            ..baseline_config()
        };
        let m = Migrator::new(cfg);
        let path = m.dump_path_or_default("prefix");
        assert_eq!(path, PathBuf::from("/config/level/dump"));
    }

    #[test]
    fn dump_path_or_default_generates_temp_path_when_no_paths() {
        let cfg = MigrationConfig {
            dump_path: None,
            ..baseline_config()
        };
        let m = Migrator::new(cfg);
        let path = m.dump_path_or_default("offline");
        assert!(path.to_string_lossy().contains("offline"));
    }

    #[test]
    fn restore_request_propagates_all_fields() {
        let cfg = MigrationConfig {
            drop_target_first: true,
            allow_restore_errors: true,
            jobs: 6,
            ..baseline_config()
        };
        let m = Migrator::new(cfg);
        let req = m.restore_request(Path::new("/tmp/dump.bin"));
        assert_eq!(req.target.host, "dst");
        assert_eq!(req.input_path, PathBuf::from("/tmp/dump.bin"));
        assert_eq!(req.jobs, 6);
        assert!(req.clean);
        assert!(req.no_owner);
        assert!(req.no_acl);
        assert!(req.tolerate_errors);
        assert!(req.section.is_none());
    }

    #[test]
    fn restore_request_defaults_no_clean_and_no_tolerate() {
        let cfg = baseline_config();
        let m = Migrator::new(cfg);
        let req = m.restore_request(Path::new("/tmp/d"));
        assert!(!req.clean);
        assert!(!req.tolerate_errors);
    }

    #[test]
    fn with_runner_replaces_runner() {
        let runner = Arc::new(RecordingRunner::default());
        let m = Migrator::new(baseline_config()).with_runner(runner.clone());
        let _ = m.config();
        assert!(runner.snapshot().is_empty());
    }

    #[test]
    fn with_reporter_replaces_reporter() {
        let reporter = Arc::new(CollectingReporter::new());
        let m = Migrator::new(baseline_config()).with_reporter(reporter);
        let _ = m.config();
    }

    #[test]
    fn resume_path_uses_config_resume_file_override() {
        let cfg = MigrationConfig {
            resume_file: Some(PathBuf::from("/custom/resume.json")),
            ..baseline_config()
        };
        let m = Migrator::new(cfg);
        let path = m.resume_path(Path::new("/tmp/dump"));
        assert_eq!(path, PathBuf::from("/custom/resume.json"));
    }

    #[test]
    fn resume_path_defaults_to_dump_path_suffix() {
        let cfg = MigrationConfig {
            resume_file: None,
            ..baseline_config()
        };
        let m = Migrator::new(cfg);
        let path = m.resume_path(Path::new("/tmp/my_dump"));
        assert_eq!(path, PathBuf::from("/tmp/my_dump.resume.json"));
    }

    #[test]
    fn dump_request_passes_none_snapshot_when_not_provided() {
        let cfg = baseline_config();
        let m = Migrator::new(cfg);
        let req = m.dump_request(Path::new("/tmp/dump"), None, None);
        assert!(req.snapshot.is_none());
    }

    #[test]
    fn dump_request_propagates_schemas_and_tables() {
        let cfg = MigrationConfig {
            schemas: vec!["public".into(), "app".into()],
            tables: vec!["public.users".into()],
            ..baseline_config()
        };
        let m = Migrator::new(cfg);
        let req = m.dump_request(Path::new("/tmp/dump"), None, None);
        assert_eq!(req.schemas, vec!["public", "app"]);
        assert_eq!(req.tables, vec!["public.users"]);
    }

    #[test]
    fn dump_request_propagates_dump_scope() {
        use crate::config::DumpScope;
        let cfg = MigrationConfig {
            dump_scope: DumpScope::SchemaOnly,
            ..baseline_config()
        };
        let m = Migrator::new(cfg);
        let req = m.dump_request(Path::new("/tmp/dump"), None, None);
        assert_eq!(req.scope, DumpScope::SchemaOnly);
    }

    #[test]
    fn migration_outcome_cutover_not_triggered_when_stats_say_no() {
        let out = MigrationOutcome {
            stats: Some(ApplyStats {
                cutover_triggered: false,
                ..ApplyStats::default()
            }),
            dump_path: PathBuf::from("/tmp/x"),
        };
        assert!(!out.cutover_triggered());
    }

    #[tokio::test]
    async fn offline_run_resume_true_no_token_on_disk_runs_from_scratch() {
        let dir = tempfile::tempdir().unwrap();
        let dump = dir.path().join("dump");
        let resume = dir.path().join("nonexistent.resume.json");

        let cfg = MigrationConfig {
            resume: true,
            dump_path: Some(dump.clone()),
            resume_file: Some(resume.clone()),
            split_sections: false,
            ..baseline_config()
        };

        let runner = Arc::new(RecordingRunner::default());
        let migrator = Migrator::new(cfg)
            .with_runner(runner.clone())
            .with_reporter(Arc::new(CollectingReporter::new()));

        migrator
            .run_offline(CancellationToken::new())
            .await
            .unwrap();

        let calls = runner.snapshot();
        assert_eq!(calls.len(), 2, "expected 2 calls (dump+restore)");
        assert_eq!(calls[0].0, "pg_dump");
        assert_eq!(calls[1].0, "pg_restore");
        // The resume token should have been written to disk.
        assert!(resume.exists());
    }

    #[tokio::test]
    async fn offline_run_writes_resume_token_to_disk() {
        let dir = tempfile::tempdir().unwrap();
        let dump_path = dir.path().join("dump_test");

        let cfg = MigrationConfig {
            dump_path: Some(dump_path.clone()),
            split_sections: false,
            ..baseline_config()
        };

        let runner = Arc::new(RecordingRunner::default());
        let migrator = Migrator::new(cfg)
            .with_runner(runner)
            .with_reporter(Arc::new(CollectingReporter::new()));

        migrator
            .run_offline(CancellationToken::new())
            .await
            .unwrap();

        // Even without --resume, the token should be saved for future use.
        let resume_path = crate::resume::default_resume_path(&dump_path);
        assert!(resume_path.exists());

        let token = crate::resume::ResumeToken::load(&resume_path)
            .await
            .unwrap()
            .unwrap();
        assert!(token.has(crate::resume::CompletedStage::Dump));
        assert!(token.has(crate::resume::CompletedStage::Restore));
    }

    #[test]
    fn dump_request_propagates_no_publications_and_no_subscriptions() {
        let cfg = MigrationConfig {
            no_publications: false,
            no_subscriptions: false,
            ..baseline_config()
        };
        let m = Migrator::new(cfg);
        let req = m.dump_request(Path::new("/tmp/dump"), None, None);
        assert!(!req.no_publications);
        assert!(!req.no_subscriptions);
    }

    #[test]
    fn dump_request_defaults_have_no_publications_true() {
        let cfg = baseline_config();
        let m = Migrator::new(cfg);
        let req = m.dump_request(Path::new("/tmp/dump"), None, None);
        assert!(req.no_publications);
        assert!(req.no_subscriptions);
    }

    #[test]
    fn migrator_debug_includes_fields() {
        let m = Migrator::new(baseline_config());
        let dbg = format!("{:?}", m);
        assert!(dbg.contains("Migrator"));
        assert!(dbg.contains("config"));
    }

    #[test]
    fn migration_outcome_dump_path_is_accessible() {
        let out = MigrationOutcome {
            stats: None,
            dump_path: PathBuf::from("/tmp/my_dump"),
        };
        assert_eq!(out.dump_path, PathBuf::from("/tmp/my_dump"));
    }

    #[tokio::test]
    async fn offline_run_reports_correct_event_sequence() {
        let runner = Arc::new(RecordingRunner::default());
        let reporter = Arc::new(CollectingReporter::new());
        let dir = tempfile::tempdir().unwrap();

        let migrator = Migrator::new(baseline_config())
            .with_runner(runner)
            .with_reporter(reporter.clone())
            .with_dump_path(dir.path().join("dump"));

        migrator
            .run_offline(CancellationToken::new())
            .await
            .unwrap();

        let events = reporter.events().await;
        let stages: Vec<_> = events.iter().map(|e| e.stage).collect();
        // Dump is first (preflight bundle is bypassed in this unit test by
        // calling run_offline directly), Complete is always last.
        assert_eq!(*stages.first().unwrap(), MigrationStage::Dump);
        assert_eq!(*stages.last().unwrap(), MigrationStage::Complete);
        // Dump comes before Restore in the sequence.
        let dump_pos = stages
            .iter()
            .position(|s| *s == MigrationStage::Dump)
            .unwrap();
        let restore_pos = stages
            .iter()
            .position(|s| *s == MigrationStage::Restore)
            .unwrap();
        assert!(dump_pos < restore_pos);
    }

    #[test]
    fn restore_request_no_owner_and_no_acl_always_true() {
        let cfg = baseline_config();
        let m = Migrator::new(cfg);
        let req = m.restore_request(Path::new("/tmp/dump"));
        assert!(req.no_owner);
        assert!(req.no_acl);
    }

    #[test]
    fn baseline_config_skips_analyze_and_vacuum() {
        let cfg = baseline_config();
        assert!(cfg.skip_analyze);
        assert!(cfg.skip_source_vacuum);
    }

    #[tokio::test]
    async fn offline_run_skips_analyze_when_skip_flags_set() {
        let runner = Arc::new(RecordingRunner::default());
        let reporter = Arc::new(CollectingReporter::new());
        let dir = tempfile::tempdir().unwrap();

        let migrator = Migrator::new(MigrationConfig {
            skip_analyze: true,
            skip_source_vacuum: true,
            split_sections: false,
            ..baseline_config()
        })
        .with_runner(runner)
        .with_reporter(reporter.clone())
        .with_dump_path(dir.path().join("dump"));

        migrator
            .run_offline(CancellationToken::new())
            .await
            .unwrap();

        let stages: Vec<_> = reporter
            .events()
            .await
            .into_iter()
            .map(|e| e.stage)
            .collect();
        assert!(!stages.contains(&MigrationStage::SourceVacuum));
        assert!(!stages.contains(&MigrationStage::Analyze));
    }

    #[tokio::test]
    async fn offline_run_skips_vacuum_when_resume_token_marks_it_done() {
        let dir = tempfile::tempdir().unwrap();
        let dump = dir.path().join("dump");
        let resume = dir.path().join("dump.resume.json");

        let cfg = MigrationConfig {
            resume: true,
            dump_path: Some(dump.clone()),
            resume_file: Some(resume.clone()),
            split_sections: false,
            skip_analyze: true,
            skip_source_vacuum: false,
            ..baseline_config()
        };

        // Pre-seed: SourceVacuum already done.
        let mut t = crate::resume::ResumeToken::new(&cfg, dump.clone());
        t.mark(crate::resume::CompletedStage::SourceVacuum);
        t.save(&resume).await.unwrap();

        let runner = Arc::new(RecordingRunner::default());
        let reporter = Arc::new(CollectingReporter::new());
        let migrator = Migrator::new(cfg)
            .with_runner(runner)
            .with_reporter(reporter.clone());

        migrator
            .run_offline(CancellationToken::new())
            .await
            .unwrap();

        let stages: Vec<_> = reporter
            .events()
            .await
            .into_iter()
            .map(|e| e.stage)
            .collect();
        assert!(
            !stages.contains(&MigrationStage::SourceVacuum),
            "SourceVacuum should be skipped on resume"
        );
    }

    #[tokio::test]
    async fn offline_run_skips_analyze_when_resume_token_marks_it_done() {
        let dir = tempfile::tempdir().unwrap();
        let dump = dir.path().join("dump");
        let resume = dir.path().join("dump.resume.json");

        let cfg = MigrationConfig {
            resume: true,
            dump_path: Some(dump.clone()),
            resume_file: Some(resume.clone()),
            split_sections: false,
            skip_analyze: false,
            skip_source_vacuum: true,
            ..baseline_config()
        };

        // Pre-seed: Dump + Restore + Analyze already done.
        let mut t = crate::resume::ResumeToken::new(&cfg, dump.clone());
        t.mark(crate::resume::CompletedStage::Dump);
        t.mark(crate::resume::CompletedStage::Restore);
        t.mark(crate::resume::CompletedStage::Analyze);
        t.save(&resume).await.unwrap();

        let runner = Arc::new(RecordingRunner::default());
        let reporter = Arc::new(CollectingReporter::new());
        let migrator = Migrator::new(cfg)
            .with_runner(runner)
            .with_reporter(reporter.clone());

        migrator
            .run_offline(CancellationToken::new())
            .await
            .unwrap();

        let stages: Vec<_> = reporter
            .events()
            .await
            .into_iter()
            .map(|e| e.stage)
            .collect();
        assert!(
            !stages.contains(&MigrationStage::Analyze),
            "Analyze should be skipped on resume"
        );
    }
}
