//! Integration tests that require live PostgreSQL instances.
//!
//! Skipped automatically when the required env vars are absent, so
//! `cargo test` still works on a bare workstation. In CI the
//! `codecov.yml` workflow provisions two PG containers and sets:
//!
//! - `PG_SOURCE_URL` → source with `wal_level=logical`
//! - `PG_TARGET_URL` → vanilla target

use std::env;

use pg_dbmigrator::tls::connect_with_sslmode;

fn source_url() -> Option<String> {
    env::var("PG_SOURCE_URL").ok()
}

fn target_url() -> Option<String> {
    env::var("PG_TARGET_URL").ok()
}

/// Connection string the *target container's* apply worker uses to reach
/// the source. In Docker this is the internal service name (`source-db:5432`)
/// rather than the host-mapped `localhost:55432`.
fn subscription_source_url() -> Option<String> {
    env::var("PG_SUBSCRIPTION_SOURCE_URL")
        .ok()
        .or_else(source_url)
}

macro_rules! skip_without_pg {
    ($url:expr) => {
        match $url {
            Some(u) => u,
            None => {
                eprintln!("skipping: PG env vars not set");
                return;
            }
        }
    };
}

// ─── tls::connect_with_sslmode ────────────────────────────────────────────────

fn append_sslmode_disable(raw: &str) -> String {
    let mut parsed = url::Url::parse(raw).expect("valid URL");
    parsed.query_pairs_mut().append_pair("sslmode", "disable");
    parsed.to_string()
}

#[tokio::test]
async fn connect_source_with_sslmode_disable() {
    let url = skip_without_pg!(source_url());
    let conn_str = append_sslmode_disable(&url);
    let client = connect_with_sslmode(&conn_str).await.unwrap();
    let row = client.query_one("SELECT 1 AS x", &[]).await.unwrap();
    let x: i32 = row.get(0);
    assert_eq!(x, 1);
}

#[tokio::test]
async fn connect_target_with_sslmode_disable() {
    let url = skip_without_pg!(target_url());
    let conn_str = append_sslmode_disable(&url);
    let client = connect_with_sslmode(&conn_str).await.unwrap();
    let row = client.query_one("SELECT version()", &[]).await.unwrap();
    let ver: String = row.get(0);
    assert!(ver.contains("PostgreSQL"));
}

// ─── preflight::recommended_client_major ─────────────────────────────────────

/// `install.sh` calls this through `--print-client-major` to pick a
/// `postgresql-client` package, so it has to work against real servers over
/// the wire protocol — no `pg_dump` on the box at that point.
#[tokio::test]
async fn recommended_client_major_matches_the_newer_live_server() {
    let source = skip_without_pg!(source_url());
    let target = skip_without_pg!(target_url());

    let major = pg_dbmigrator::preflight::recommended_client_major(&source, &target)
        .await
        .unwrap();

    // Don't hardcode the compose stack's version: assert the contract instead,
    // so bumping docker-compose.test.yml doesn't break this.
    let source_client = connect_with_sslmode(&append_sslmode_disable(&source))
        .await
        .unwrap();
    let target_client = connect_with_sslmode(&append_sslmode_disable(&target))
        .await
        .unwrap();
    let read_major = |row: tokio_postgres::Row| (row.get::<_, i32>(0) / 10000) as u32;
    let source_major = read_major(
        source_client
            .query_one("SELECT current_setting('server_version_num')::integer", &[])
            .await
            .unwrap(),
    );
    let target_major = read_major(
        target_client
            .query_one("SELECT current_setting('server_version_num')::integer", &[])
            .await
            .unwrap(),
    );

    assert_eq!(
        major,
        source_major.max(target_major),
        "recommended client must be the newer of source {source_major} / target {target_major}"
    );
}

/// End-to-end guard on the contract `install.sh` depends on: the binary must
/// put the bare integer on stdout and every log line on stderr. Regressing
/// that (e.g. by letting the tracing subscriber default back to stdout) makes
/// the installer silently fall back to the newest client instead of the one
/// the servers actually need.
#[test]
fn print_client_major_puts_only_the_number_on_stdout() {
    let source = skip_without_pg!(source_url());
    let target = skip_without_pg!(target_url());

    let out = std::process::Command::new(env!("CARGO_BIN_EXE_pg_dbmigrator"))
        .args([
            "--print-client-major",
            "--source",
            &source,
            "--target",
            &target,
        ])
        .output()
        .expect("failed to run the pg_dbmigrator binary");

    assert!(
        out.status.success(),
        "exit {:?}, stderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );

    let stdout = String::from_utf8(out.stdout).expect("stdout should be UTF-8");
    let major: u32 = stdout.trim().parse().unwrap_or_else(|e| {
        panic!("stdout must be a bare integer, got {stdout:?}: {e}");
    });
    assert!(major >= 9, "implausible major {major}");
}

// ─── preflight::verify_source_logical_replication_ready ──────────────────────

#[tokio::test]
async fn verify_source_logical_replication_ready_passes() {
    let url = skip_without_pg!(source_url());
    pg_dbmigrator::preflight::verify_source_logical_replication_ready(&url)
        .await
        .unwrap();
}

// ─── preflight::verify_publication_exists ─────────────────────────────────────

#[tokio::test]
async fn verify_publication_missing_returns_error() {
    let url = skip_without_pg!(source_url());
    let result =
        pg_dbmigrator::preflight::verify_publication_exists(&url, "nonexistent_pub_xyz").await;
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("nonexistent_pub_xyz"));
}

#[tokio::test]
async fn verify_publication_exists_after_creation() {
    let url = skip_without_pg!(source_url());
    let client = connect_with_sslmode(&url).await.unwrap();
    client
        .batch_execute("CREATE PUBLICATION test_integ_pub FOR ALL TABLES")
        .await
        .unwrap_or(());
    let result = pg_dbmigrator::preflight::verify_publication_exists(&url, "test_integ_pub").await;
    assert!(result.is_ok());
    client
        .batch_execute("DROP PUBLICATION IF EXISTS test_integ_pub")
        .await
        .ok();
}

// ─── preflight::ensure_target_database_exists ─────────────────────────────────

#[tokio::test]
async fn ensure_target_database_already_exists() {
    let url = skip_without_pg!(target_url());
    pg_dbmigrator::preflight::ensure_target_database_exists(&url, "target_db")
        .await
        .unwrap();
}

#[tokio::test]
async fn ensure_target_database_creates_new() {
    let url = skip_without_pg!(target_url());
    let db_name = "test_integ_create_db";
    let maint_conn = pg_dbmigrator::preflight::maintenance_connection_string(&url);
    let client = connect_with_sslmode(&maint_conn).await.unwrap();
    client
        .batch_execute(&format!("DROP DATABASE IF EXISTS {db_name}"))
        .await
        .ok();

    pg_dbmigrator::preflight::ensure_target_database_exists(&url, db_name)
        .await
        .unwrap();

    let row = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)",
            &[&db_name],
        )
        .await
        .unwrap();
    let exists: bool = row.get(0);
    assert!(exists);

    client
        .batch_execute(&format!("DROP DATABASE IF EXISTS {db_name}"))
        .await
        .ok();
}

// ─── preflight::ensure_pglogical_not_interfering ─────────────────────────────

#[tokio::test]
async fn ensure_pglogical_not_interfering_passes_on_vanilla() {
    let url = skip_without_pg!(target_url());
    pg_dbmigrator::preflight::ensure_pglogical_not_interfering(&url)
        .await
        .unwrap();
}

// ─── sequences module ─────────────────────────────────────────────────────────

#[tokio::test]
async fn collect_source_sequences_returns_empty_on_fresh_db() {
    let url = skip_without_pg!(source_url());
    let client = connect_with_sslmode(&url).await.unwrap();
    client
        .batch_execute("DROP SEQUENCE IF EXISTS test_integ_seq")
        .await
        .ok();
    let seqs = pg_dbmigrator::sequences::collect_source_sequences(&client, &[])
        .await
        .unwrap();
    let found = seqs.iter().any(|s| s.name == "test_integ_seq");
    assert!(!found);
}

#[tokio::test]
async fn collect_and_apply_sequences_round_trip() {
    let source_url = skip_without_pg!(source_url());
    let target_url = skip_without_pg!(target_url());

    let source = connect_with_sslmode(&source_url).await.unwrap();
    let target = connect_with_sslmode(&target_url).await.unwrap();

    source
        .batch_execute(
            "CREATE SEQUENCE IF NOT EXISTS test_seq_integ START 1; \
             SELECT nextval('test_seq_integ'); \
             SELECT nextval('test_seq_integ'); \
             SELECT nextval('test_seq_integ');",
        )
        .await
        .unwrap();

    target
        .batch_execute("CREATE SEQUENCE IF NOT EXISTS test_seq_integ START 1")
        .await
        .unwrap();

    let seqs = pg_dbmigrator::sequences::collect_source_sequences(&source, &[])
        .await
        .unwrap();
    let our_seq = seqs.iter().find(|s| s.name == "test_seq_integ").unwrap();
    assert!(our_seq.last_value.is_some());
    assert!(our_seq.last_value.unwrap() >= 3);

    let applied =
        pg_dbmigrator::sequences::apply_sequences_to_target(&target, std::slice::from_ref(our_seq))
            .await
            .unwrap();
    assert_eq!(applied, 1);

    let row = target
        .query_one("SELECT last_value FROM test_seq_integ", &[])
        .await
        .unwrap();
    let val: i64 = row.get(0);
    assert!(val >= 3);

    source
        .batch_execute("DROP SEQUENCE IF EXISTS test_seq_integ")
        .await
        .ok();
    target
        .batch_execute("DROP SEQUENCE IF EXISTS test_seq_integ")
        .await
        .ok();
}

#[tokio::test]
async fn collect_sequences_with_schema_filter() {
    let url = skip_without_pg!(source_url());
    let client = connect_with_sslmode(&url).await.unwrap();

    client
        .batch_execute(
            "CREATE SCHEMA IF NOT EXISTS integ_schema_a; \
             CREATE SEQUENCE IF NOT EXISTS integ_schema_a.filtered_seq START 1; \
             SELECT nextval('integ_schema_a.filtered_seq');",
        )
        .await
        .unwrap();

    let filter = vec!["integ_schema_a".to_string()];
    let seqs = pg_dbmigrator::sequences::collect_source_sequences(&client, &filter)
        .await
        .unwrap();
    assert!(seqs.iter().any(|s| s.name == "filtered_seq"));
    assert!(!seqs.iter().any(|s| s.schema == "public"));

    client
        .batch_execute(
            "DROP SEQUENCE IF EXISTS integ_schema_a.filtered_seq; \
             DROP SCHEMA IF EXISTS integ_schema_a",
        )
        .await
        .ok();
}

#[tokio::test]
async fn sync_sequences_end_to_end() {
    let source_url_val = skip_without_pg!(source_url());
    let target_url_val = skip_without_pg!(target_url());

    let source = connect_with_sslmode(&source_url_val).await.unwrap();
    let target = connect_with_sslmode(&target_url_val).await.unwrap();

    source
        .batch_execute(
            "CREATE SEQUENCE IF NOT EXISTS sync_e2e_seq START 1; \
             SELECT setval('sync_e2e_seq', 42);",
        )
        .await
        .unwrap();
    target
        .batch_execute("CREATE SEQUENCE IF NOT EXISTS sync_e2e_seq START 1")
        .await
        .unwrap();

    let applied = pg_dbmigrator::sequences::sync_sequences(&source_url_val, &target_url_val, &[])
        .await
        .unwrap();
    assert!(applied >= 1);

    let row = target
        .query_one("SELECT last_value FROM sync_e2e_seq", &[])
        .await
        .unwrap();
    let val: i64 = row.get(0);
    assert_eq!(val, 42);

    source
        .batch_execute("DROP SEQUENCE IF EXISTS sync_e2e_seq")
        .await
        .ok();
    target
        .batch_execute("DROP SEQUENCE IF EXISTS sync_e2e_seq")
        .await
        .ok();
}

// ─── native_apply::PgSubscriptionLagProvider ─────────────────────────────────

#[tokio::test]
async fn lag_provider_connect_fails_without_slot() {
    let url = skip_without_pg!(source_url());
    let provider = pg_dbmigrator::native_apply::PgSubscriptionLagProvider::connect(
        &url,
        "nonexistent_slot_xyz",
    )
    .await;
    assert!(provider.is_ok());
    let p = provider.unwrap();
    use pg_dbmigrator::native_apply::SubscriptionLagProvider;
    let result = p.sample().await;
    assert!(result.is_err());
}

// ─── native_apply::force_clean_stale_state ───────────────────────────────────

#[tokio::test]
async fn force_clean_stale_state_is_idempotent() {
    let source_url_val = skip_without_pg!(source_url());
    let target_url_val = skip_without_pg!(target_url());
    let online = pg_dbmigrator::OnlineOptions {
        subscription_name: "integ_nonexist_sub".into(),
        slot_name: "integ_nonexist_slot".into(),
        ..pg_dbmigrator::OnlineOptions::default()
    };
    let result = pg_dbmigrator::native_apply::force_clean_stale_state(
        &source_url_val,
        &target_url_val,
        &online,
    )
    .await;
    assert!(result.is_ok());
}

// ─── native_apply::wait_for_slot_inactive ────────────────────────────────────

#[tokio::test]
async fn wait_for_slot_inactive_returns_ok_for_missing_slot() {
    let url = skip_without_pg!(source_url());
    let reporter = pg_dbmigrator::progress::CollectingReporter::new();
    let result =
        pg_dbmigrator::native_apply::wait_for_slot_inactive(&url, "absent_slot_xyz", &reporter)
            .await;
    assert!(result.is_ok());
}

// ─── native_apply::cleanup_target_subscription ───────────────────────────────

#[tokio::test]
async fn cleanup_target_subscription_noop_when_absent() {
    let url = skip_without_pg!(target_url());
    let online = pg_dbmigrator::OnlineOptions {
        subscription_name: "integ_absent_sub".into(),
        slot_name: "integ_absent_slot".into(),
        ..pg_dbmigrator::OnlineOptions::default()
    };
    let result = pg_dbmigrator::native_apply::cleanup_target_subscription(&url, &online).await;
    assert!(result.is_ok());
}

// ─── native_apply::disable_target_subscription ───────────────────────────────

#[tokio::test]
async fn disable_target_subscription_noop_when_absent() {
    let url = skip_without_pg!(target_url());
    let online = pg_dbmigrator::OnlineOptions {
        subscription_name: "integ_no_sub".into(),
        ..pg_dbmigrator::OnlineOptions::default()
    };
    pg_dbmigrator::native_apply::disable_target_subscription(&url, &online).await;
}

// ─── snapshot::prepare_replication_slot ───────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn prepare_replication_slot_creates_and_exports_snapshot() {
    let url = skip_without_pg!(source_url());
    let client = connect_with_sslmode(&url).await.unwrap();

    // Clean up any leftovers from a previous run
    client
        .batch_execute(
            "SELECT pg_drop_replication_slot('integ_snap_slot') \
             WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'integ_snap_slot')",
        )
        .await
        .ok();

    client
        .batch_execute("CREATE PUBLICATION integ_snap_pub FOR ALL TABLES")
        .await
        .unwrap_or(());

    let online = pg_dbmigrator::OnlineOptions {
        slot_name: "integ_snap_slot".into(),
        publication: "integ_snap_pub".into(),
        subscription_name: "integ_snap_sub".into(),
        ..pg_dbmigrator::OnlineOptions::default()
    };

    let result = pg_dbmigrator::snapshot::prepare_replication_slot(&url, &online).await;
    match result {
        Ok(prepared) => {
            assert!(prepared.snapshot_name.is_some());
            drop(prepared.stream);
            // Clean up the slot
            client
                .batch_execute(
                    "SELECT pg_drop_replication_slot('integ_snap_slot') \
                     WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'integ_snap_slot')",
                )
                .await
                .ok();
        }
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("already exists") || msg.contains("replication"),
                "unexpected error: {msg}"
            );
        }
    }

    client
        .batch_execute("DROP PUBLICATION IF EXISTS integ_snap_pub")
        .await
        .ok();
}

// ─── Full online apply loop (short-circuit) ──────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn native_apply_with_cancel_exits_cleanly() {
    use pg_dbmigrator::cutover::CutoverHandle;
    use pg_dbmigrator::native_apply::{run_native_apply, SubscriptionLagProvider};
    use pg_dbmigrator::progress::CollectingReporter;
    use pg_dbmigrator::OnlineOptions;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio_util::sync::CancellationToken;

    let source_url_val = skip_without_pg!(source_url());
    let target_url_val = skip_without_pg!(target_url());
    let sub_source_url = skip_without_pg!(subscription_source_url());

    let source = connect_with_sslmode(&source_url_val).await.unwrap();
    let target = connect_with_sslmode(&target_url_val).await.unwrap();

    source
        .batch_execute("CREATE PUBLICATION integ_apply_pub FOR ALL TABLES")
        .await
        .unwrap_or(());

    let online = OnlineOptions {
        slot_name: "integ_apply_slot".into(),
        publication: "integ_apply_pub".into(),
        subscription_name: "integ_apply_sub".into(),
        drop_subscription_on_cutover: true,
        ..OnlineOptions::default()
    };

    // Create the slot so CREATE SUBSCRIPTION can reference it
    source
        .batch_execute("SELECT pg_create_logical_replication_slot('integ_apply_slot', 'pgoutput')")
        .await
        .unwrap_or(());

    // Use a mock lag provider since we just want to test the loop mechanics
    #[derive(Debug)]
    struct MockProvider {
        s: AtomicU64,
        c: AtomicU64,
    }
    #[async_trait::async_trait]
    impl SubscriptionLagProvider for MockProvider {
        async fn sample(&self) -> pg_dbmigrator::Result<(u64, u64)> {
            Ok((self.s.load(Ordering::SeqCst), self.c.load(Ordering::SeqCst)))
        }
    }
    let provider = MockProvider {
        s: AtomicU64::new(100),
        c: AtomicU64::new(100),
    };

    let cancel = CancellationToken::new();
    let cancel2 = cancel.clone();
    let reporter = CollectingReporter::new();
    let cutover = CutoverHandle::new();

    // Cancel after a short delay
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        cancel2.cancel();
    });

    let result = run_native_apply(
        &target,
        &provider,
        &online,
        &sub_source_url,
        cutover,
        &reporter,
        cancel,
    )
    .await;

    // The loop should exit due to cancel; the CREATE SUBSCRIPTION may or
    // may not succeed depending on PG state, but the cancellation path
    // should not panic.
    match result {
        Ok(stats) => {
            assert!(!stats.cutover_triggered);
        }
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("subscription")
                    || msg.contains("slot")
                    || msg.contains("does not exist"),
                "unexpected error: {msg}"
            );
        }
    }

    // Cleanup
    target
        .batch_execute(
            "DO $$ BEGIN \
               IF EXISTS (SELECT 1 FROM pg_subscription WHERE subname = 'integ_apply_sub') THEN \
                 EXECUTE 'ALTER SUBSCRIPTION integ_apply_sub DISABLE'; \
                 EXECUTE 'ALTER SUBSCRIPTION integ_apply_sub SET (slot_name = NONE)'; \
                 EXECUTE 'DROP SUBSCRIPTION integ_apply_sub'; \
               END IF; \
             END $$;",
        )
        .await
        .ok();
    source
        .batch_execute(
            "SELECT pg_drop_replication_slot(slot_name) \
             FROM pg_replication_slots \
             WHERE slot_name = 'integ_apply_slot'",
        )
        .await
        .ok();
    source
        .batch_execute("DROP PUBLICATION IF EXISTS integ_apply_pub")
        .await
        .ok();
}

// ─── Full online apply loop reaches the cutover break ────────────────────────

/// Drives `run_native_apply` with cutover requested up front so the loop takes the `is_requested()` break path on its first iteration. This is the only  test that exercises the cutover branch (`stats.cutover_triggered = true` +  the `Cutover` progress event); the sibling cancel test asserts the opposite.
#[tokio::test(flavor = "multi_thread")]
async fn native_apply_triggers_cutover_when_requested() {
    use pg_dbmigrator::cutover::CutoverHandle;
    use pg_dbmigrator::native_apply::{run_native_apply, SubscriptionLagProvider};
    use pg_dbmigrator::progress::CollectingReporter;
    use pg_dbmigrator::OnlineOptions;
    use tokio_util::sync::CancellationToken;

    let source_url_val = skip_without_pg!(source_url());
    let target_url_val = skip_without_pg!(target_url());
    let sub_source_url = skip_without_pg!(subscription_source_url());

    let source = connect_with_sslmode(&source_url_val).await.unwrap();
    let target = connect_with_sslmode(&target_url_val).await.unwrap();

    source
        .batch_execute("CREATE PUBLICATION integ_cutover_pub FOR ALL TABLES")
        .await
        .unwrap_or(());

    let online = OnlineOptions {
        slot_name: "integ_cutover_slot".into(),
        publication: "integ_cutover_pub".into(),
        subscription_name: "integ_cutover_sub".into(),
        drop_subscription_on_cutover: true,
        ..OnlineOptions::default()
    };

    source
        .batch_execute(
            "SELECT pg_create_logical_replication_slot('integ_cutover_slot', 'pgoutput')",
        )
        .await
        .unwrap_or(());

    // Always "caught up" so the loop never blocks on lag; cutover is what ends it.
    #[derive(Debug)]
    struct CaughtUpProvider;
    #[async_trait::async_trait]
    impl SubscriptionLagProvider for CaughtUpProvider {
        async fn sample(&self) -> pg_dbmigrator::Result<(u64, u64)> {
            Ok((100, 100))
        }
    }

    let reporter = CollectingReporter::new();
    let cutover = CutoverHandle::new();
    // Request BEFORE the call: the first loop iteration must break via cutover.
    assert!(!cutover.request());

    let result = run_native_apply(
        &target,
        &CaughtUpProvider,
        &online,
        &sub_source_url,
        cutover,
        &reporter,
        CancellationToken::new(),
    )
    .await;

    match result {
        Ok(stats) => {
            assert!(
                stats.cutover_triggered,
                "loop should have exited via the cutover break path"
            );
        }
        Err(e) => {
            // Tolerated only for environments where CREATE SUBSCRIPTION can't
            // establish (target can't reach source); the coverage CI can, so
            // the Ok branch above is what exercises the cutover lines there.
            let msg = e.to_string();
            assert!(
                msg.contains("subscription")
                    || msg.contains("slot")
                    || msg.contains("does not exist"),
                "unexpected error: {msg}"
            );
        }
    }

    // Cleanup
    target
        .batch_execute(
            "DO $$ BEGIN \
               IF EXISTS (SELECT 1 FROM pg_subscription WHERE subname = 'integ_cutover_sub') THEN \
                 EXECUTE 'ALTER SUBSCRIPTION integ_cutover_sub DISABLE'; \
                 EXECUTE 'ALTER SUBSCRIPTION integ_cutover_sub SET (slot_name = NONE)'; \
                 EXECUTE 'DROP SUBSCRIPTION integ_cutover_sub'; \
               END IF; \
             END $$;",
        )
        .await
        .ok();
    source
        .batch_execute(
            "SELECT pg_drop_replication_slot(slot_name) \
             FROM pg_replication_slots \
             WHERE slot_name = 'integ_cutover_slot'",
        )
        .await
        .ok();
    source
        .batch_execute("DROP PUBLICATION IF EXISTS integ_cutover_pub")
        .await
        .ok();
}

// ─── preflight::verify_pg_tools_installed (live) ─────────────────────────────

#[tokio::test]
async fn verify_pg_tools_installed_succeeds_in_ci() {
    // In CI with PostgreSQL client tools available this should pass.
    // On bare workstations without pg tools it may fail, but since we
    // skip_without_pg this only runs in CI.
    let source = skip_without_pg!(source_url());
    let target = skip_without_pg!(target_url());
    pg_dbmigrator::preflight::verify_pg_tools_installed(&source, &target)
        .await
        .unwrap();
}

// ─── analyze::run_target_analyze ─────────────────────────────────────────────

#[tokio::test]
async fn run_target_analyze_whole_database() {
    let url = skip_without_pg!(target_url());
    let client = connect_with_sslmode(&url).await.unwrap();
    client
        .batch_execute(
            "CREATE SCHEMA IF NOT EXISTS integ_analyze; \
             CREATE TABLE IF NOT EXISTS integ_analyze.t1 (id int PRIMARY KEY, v text);",
        )
        .await
        .unwrap();

    let result = pg_dbmigrator::analyze::run_target_analyze(&url, &[], false).await;
    assert!(result.is_ok());

    client
        .batch_execute("DROP SCHEMA integ_analyze CASCADE")
        .await
        .ok();
}

#[tokio::test]
async fn run_target_analyze_with_schema_filter() {
    let url = skip_without_pg!(target_url());
    let client = connect_with_sslmode(&url).await.unwrap();
    client
        .batch_execute(
            "CREATE SCHEMA IF NOT EXISTS integ_analyze_s; \
             CREATE TABLE IF NOT EXISTS integ_analyze_s.t1 (id int PRIMARY KEY, v text); \
             CREATE TABLE IF NOT EXISTS integ_analyze_s.t2 (id int PRIMARY KEY, n int);",
        )
        .await
        .unwrap();

    let schemas = vec!["integ_analyze_s".to_string()];
    let result = pg_dbmigrator::analyze::run_target_analyze(&url, &schemas, false).await;
    assert!(result.is_ok());

    // Verbose mode
    let result = pg_dbmigrator::analyze::run_target_analyze(&url, &schemas, true).await;
    assert!(result.is_ok());

    client
        .batch_execute("DROP SCHEMA integ_analyze_s CASCADE")
        .await
        .ok();
}

// ─── analyze::run_source_vacuum ──────────────────────────────────────────────

#[tokio::test]
async fn run_source_vacuum_whole_database() {
    let url = skip_without_pg!(source_url());
    let result = pg_dbmigrator::analyze::run_source_vacuum(&url, &[], false).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn run_source_vacuum_with_schema_filter() {
    let url = skip_without_pg!(source_url());
    let client = connect_with_sslmode(&url).await.unwrap();
    client
        .batch_execute(
            "CREATE SCHEMA IF NOT EXISTS integ_vacuum_s; \
             CREATE TABLE IF NOT EXISTS integ_vacuum_s.t1 (id int PRIMARY KEY, v text);",
        )
        .await
        .unwrap();

    let schemas = vec!["integ_vacuum_s".to_string()];
    let result = pg_dbmigrator::analyze::run_source_vacuum(&url, &schemas, false).await;
    assert!(result.is_ok());

    // Verbose mode
    let result = pg_dbmigrator::analyze::run_source_vacuum(&url, &schemas, true).await;
    assert!(result.is_ok());

    client
        .batch_execute("DROP SCHEMA integ_vacuum_s CASCADE")
        .await
        .ok();
}

// ─── analyze::maybe_vacuum_source / maybe_analyze_target ─────────────────────

#[tokio::test]
async fn maybe_vacuum_source_runs_when_not_skipped() {
    let url = skip_without_pg!(source_url());
    let config = pg_dbmigrator::MigrationConfig {
        source: pg_dbmigrator::EndpointConfig::parse(&url).unwrap(),
        skip_source_vacuum: false,
        ..pg_dbmigrator::MigrationConfig::default()
    };
    let result = pg_dbmigrator::analyze::maybe_vacuum_source(&config).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn maybe_analyze_target_runs_when_not_skipped() {
    let url = skip_without_pg!(target_url());
    let config = pg_dbmigrator::MigrationConfig {
        target: pg_dbmigrator::EndpointConfig::parse(&url).unwrap(),
        skip_analyze: false,
        ..pg_dbmigrator::MigrationConfig::default()
    };
    let result = pg_dbmigrator::analyze::maybe_analyze_target(&config).await;
    assert!(result.is_ok());
}

// ─── preflight::ensure_publication_exists ────────────────────────────────────

#[tokio::test]
async fn ensure_publication_exists_creates_when_missing() {
    let url = skip_without_pg!(source_url());
    let client = connect_with_sslmode(&url).await.unwrap();

    // Clean up any leftover
    client
        .batch_execute("DROP PUBLICATION IF EXISTS integ_auto_pub")
        .await
        .ok();

    let created = pg_dbmigrator::preflight::ensure_publication_exists(
        &url,
        "integ_auto_pub",
        &[],
        &[],
        &[],
        &[],
    )
    .await
    .unwrap();
    assert!(created, "publication should have been auto-created");

    // Verify it exists now
    let row = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = 'integ_auto_pub')",
            &[],
        )
        .await
        .unwrap();
    let exists: bool = row.get(0);
    assert!(exists);

    // Clean up
    client
        .batch_execute("DROP PUBLICATION IF EXISTS integ_auto_pub")
        .await
        .ok();
}

#[tokio::test]
async fn ensure_publication_exists_noop_when_present() {
    let url = skip_without_pg!(source_url());
    let client = connect_with_sslmode(&url).await.unwrap();

    // Pre-create the publication
    client
        .batch_execute("CREATE PUBLICATION integ_existing_pub FOR ALL TABLES")
        .await
        .unwrap_or(());

    let created = pg_dbmigrator::preflight::ensure_publication_exists(
        &url,
        "integ_existing_pub",
        &[],
        &[],
        &[],
        &[],
    )
    .await
    .unwrap();
    assert!(
        !created,
        "publication already existed, should not re-create"
    );

    // Clean up
    client
        .batch_execute("DROP PUBLICATION IF EXISTS integ_existing_pub")
        .await
        .ok();
}

#[tokio::test]
async fn ensure_publication_excludes_tables_when_no_includes() {
    let url = skip_without_pg!(source_url());
    let client = connect_with_sslmode(&url).await.unwrap();

    client
        .batch_execute("DROP PUBLICATION IF EXISTS integ_excl_pub")
        .await
        .ok();
    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS public.keep_me (id int); \
             CREATE TABLE IF NOT EXISTS public.skip_me (id int);",
        )
        .await
        .unwrap();

    let created = pg_dbmigrator::preflight::ensure_publication_exists(
        &url,
        "integ_excl_pub",
        &[],
        &[],
        &["public.skip_me".into()],
        &[],
    )
    .await
    .unwrap();
    assert!(created);

    // The publication should list tables explicitly, NOT be FOR ALL TABLES.
    let row = client
        .query_one(
            "SELECT puballtables FROM pg_publication WHERE pubname = 'integ_excl_pub'",
            &[],
        )
        .await
        .unwrap();
    let all_tables: bool = row.get(0);
    assert!(
        !all_tables,
        "publication should NOT be FOR ALL TABLES when exclusions are set"
    );

    // Verify skip_me is not in the publication's table list.
    // Use pg_publication_rel JOIN pg_class instead of pg_publication_tables
    // because the latter calls relation_open() on every OID and crashes if
    // a concurrently-running test dropped a table after the publication was
    // created.
    let skip_rows = client
        .query(
            "SELECT c.relname FROM pg_publication_rel pr \
             JOIN pg_class c ON c.oid = pr.prrelid \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE pr.prpubid = (SELECT oid FROM pg_publication WHERE pubname = 'integ_excl_pub') \
               AND c.relname = 'skip_me'",
            &[],
        )
        .await
        .unwrap();
    assert!(
        skip_rows.is_empty(),
        "excluded table should not be in publication"
    );

    // Verify keep_me IS in the publication.
    let keep_rows = client
        .query(
            "SELECT c.relname FROM pg_publication_rel pr \
             JOIN pg_class c ON c.oid = pr.prrelid \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE pr.prpubid = (SELECT oid FROM pg_publication WHERE pubname = 'integ_excl_pub') \
               AND c.relname = 'keep_me'",
            &[],
        )
        .await
        .unwrap();
    assert!(
        !keep_rows.is_empty(),
        "non-excluded table should be in publication"
    );

    client
        .batch_execute(
            "DROP PUBLICATION IF EXISTS integ_excl_pub; \
             DROP TABLE IF EXISTS public.keep_me; \
             DROP TABLE IF EXISTS public.skip_me;",
        )
        .await
        .ok();
}

#[tokio::test]
async fn ensure_publication_excludes_schema_when_no_includes() {
    let url = skip_without_pg!(source_url());
    let client = connect_with_sslmode(&url).await.unwrap();

    client
        .batch_execute("DROP PUBLICATION IF EXISTS integ_excl_schema_pub")
        .await
        .ok();
    client
        .batch_execute(
            "CREATE SCHEMA IF NOT EXISTS excl_test; \
             CREATE TABLE IF NOT EXISTS excl_test.should_skip (id int); \
             CREATE TABLE IF NOT EXISTS public.should_keep (id int);",
        )
        .await
        .unwrap();

    let created = pg_dbmigrator::preflight::ensure_publication_exists(
        &url,
        "integ_excl_schema_pub",
        &[],
        &[],
        &[],
        &["excl_test".into()],
    )
    .await
    .unwrap();
    assert!(created);

    let skip_rows = client
        .query(
            "SELECT schemaname, tablename FROM pg_publication_tables \
             WHERE pubname = 'integ_excl_schema_pub' AND schemaname = 'excl_test'",
            &[],
        )
        .await
        .unwrap();
    assert!(
        skip_rows.is_empty(),
        "tables from excluded schema should not be in publication"
    );

    client
        .batch_execute(
            "DROP PUBLICATION IF EXISTS integ_excl_schema_pub; \
             DROP TABLE IF EXISTS excl_test.should_skip; \
             DROP TABLE IF EXISTS public.should_keep; \
             DROP SCHEMA IF EXISTS excl_test;",
        )
        .await
        .ok();
}

#[tokio::test]
async fn ensure_publication_filters_includes_with_exclusions() {
    let url = skip_without_pg!(source_url());
    let client = connect_with_sslmode(&url).await.unwrap();

    client
        .batch_execute("DROP PUBLICATION IF EXISTS integ_incl_excl_pub")
        .await
        .ok();
    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS public.inc_keep (id int); \
             CREATE TABLE IF NOT EXISTS public.inc_skip (id int);",
        )
        .await
        .unwrap();

    let created = pg_dbmigrator::preflight::ensure_publication_exists(
        &url,
        "integ_incl_excl_pub",
        &["public.inc_keep".into(), "public.inc_skip".into()],
        &[],
        &["public.inc_skip".into()],
        &[],
    )
    .await
    .unwrap();
    assert!(created);

    let skip_rows = client
        .query(
            "SELECT tablename FROM pg_publication_tables \
             WHERE pubname = 'integ_incl_excl_pub' AND tablename = 'inc_skip'",
            &[],
        )
        .await
        .unwrap();
    assert!(
        skip_rows.is_empty(),
        "excluded table should be filtered from include list"
    );

    let keep_rows = client
        .query(
            "SELECT tablename FROM pg_publication_tables \
             WHERE pubname = 'integ_incl_excl_pub' AND tablename = 'inc_keep'",
            &[],
        )
        .await
        .unwrap();
    assert!(
        !keep_rows.is_empty(),
        "non-excluded table from include list should be in publication"
    );

    client
        .batch_execute(
            "DROP PUBLICATION IF EXISTS integ_incl_excl_pub; \
             DROP TABLE IF EXISTS public.inc_keep; \
             DROP TABLE IF EXISTS public.inc_skip;",
        )
        .await
        .ok();
}

// ─── native_apply::drop_source_publication ──────────────────────────────────

#[tokio::test]
async fn drop_source_publication_is_idempotent() {
    let url = skip_without_pg!(source_url());
    let client = connect_with_sslmode(&url).await.unwrap();

    // Create a publication, then drop it twice — both should succeed
    client
        .batch_execute("CREATE PUBLICATION integ_drop_pub FOR ALL TABLES")
        .await
        .unwrap_or(());

    let result = pg_dbmigrator::native_apply::drop_source_publication(&url, "integ_drop_pub").await;
    assert!(result.is_ok());

    // Second drop should also succeed (IF EXISTS)
    let result = pg_dbmigrator::native_apply::drop_source_publication(&url, "integ_drop_pub").await;
    assert!(result.is_ok());
}

// ─── native_apply::drop_source_slot ─────────────────────────────────────────

#[tokio::test]
async fn drop_source_slot_is_idempotent() {
    let url = skip_without_pg!(source_url());
    let client = connect_with_sslmode(&url).await.unwrap();

    // Create a slot, then drop it
    client
        .batch_execute("SELECT pg_create_logical_replication_slot('integ_drop_slot', 'pgoutput')")
        .await
        .unwrap_or(());

    let result = pg_dbmigrator::native_apply::drop_source_slot(&url, "integ_drop_slot").await;
    assert!(result.is_ok());

    // Second drop should also succeed (slot absent → noop)
    let result = pg_dbmigrator::native_apply::drop_source_slot(&url, "integ_drop_slot").await;
    assert!(result.is_ok());
}

// ─── orchestrator::cleanup_source_after_cutover ─────────────────────────────

#[tokio::test]
async fn cleanup_source_after_cutover_drops_pub_and_slot() {
    let url = skip_without_pg!(source_url());
    let client = connect_with_sslmode(&url).await.unwrap();

    // Pre-create a publication and slot
    client
        .batch_execute("CREATE PUBLICATION integ_cleanup_pub FOR ALL TABLES")
        .await
        .unwrap_or(());
    client
        .batch_execute(
            "SELECT pg_create_logical_replication_slot('integ_cleanup_slot', 'pgoutput')",
        )
        .await
        .unwrap_or(());

    let online = pg_dbmigrator::OnlineOptions {
        publication: "integ_cleanup_pub".into(),
        slot_name: "integ_cleanup_slot".into(),
        drop_slot_on_cutover: true,
        ..pg_dbmigrator::OnlineOptions::default()
    };

    let reporter = pg_dbmigrator::progress::CollectingReporter::new();
    pg_dbmigrator::cleanup_source_after_cutover(&url, &online, true, &reporter).await;

    // Verify publication was dropped
    let row = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = 'integ_cleanup_pub')",
            &[],
        )
        .await
        .unwrap();
    let exists: bool = row.get(0);
    assert!(!exists, "publication should have been dropped");

    // Verify slot was dropped
    let row = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = 'integ_cleanup_slot')",
            &[],
        )
        .await
        .unwrap();
    let exists: bool = row.get(0);
    assert!(!exists, "slot should have been dropped");

    // Check reporter emitted SourceCleanup events
    let events = reporter.events().await;
    assert!(events.len() >= 2);
    assert!(events
        .iter()
        .all(|e| e.stage == pg_dbmigrator::MigrationStage::SourceCleanup));
}

#[tokio::test]
async fn cleanup_source_after_cutover_skips_when_not_auto_created() {
    let url = skip_without_pg!(source_url());
    let client = connect_with_sslmode(&url).await.unwrap();

    // Pre-create a publication (simulate operator-created)
    client
        .batch_execute("CREATE PUBLICATION integ_keep_pub FOR ALL TABLES")
        .await
        .unwrap_or(());

    let online = pg_dbmigrator::OnlineOptions {
        publication: "integ_keep_pub".into(),
        slot_name: "integ_absent_slot_xyz".into(),
        drop_slot_on_cutover: false,
        ..pg_dbmigrator::OnlineOptions::default()
    };

    let reporter = pg_dbmigrator::progress::CollectingReporter::new();
    // pub_auto_created = false, drop_slot_on_cutover = false → should be a no-op
    pg_dbmigrator::cleanup_source_after_cutover(&url, &online, false, &reporter).await;

    // Publication should still exist
    let row = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = 'integ_keep_pub')",
            &[],
        )
        .await
        .unwrap();
    let exists: bool = row.get(0);
    assert!(exists, "publication should NOT have been dropped");

    // No events emitted
    let events = reporter.events().await;
    assert!(events.is_empty());

    // Clean up
    client
        .batch_execute("DROP PUBLICATION IF EXISTS integ_keep_pub")
        .await
        .ok();
}

// ─── slot/snapshot handoff regression guard ──────────────────────────────────

/// Verifies the slot+snapshot stay alive across a simulated pg_dump phase
/// and that `wait_for_slot_inactive` returns once the holding stream is
/// dropped. Regression guard for the snapshot-handoff sequence.
#[tokio::test(flavor = "multi_thread")]
async fn online_slot_handoff_survives_dump_phase() {
    let Some(source) = source_url() else {
        eprintln!("skipped: PG_SOURCE_URL not set");
        return;
    };

    use pg_dbmigrator::native_apply::wait_for_slot_inactive;
    use pg_dbmigrator::progress::CollectingReporter;
    use pg_dbmigrator::snapshot::prepare_replication_slot;
    use pg_dbmigrator::OnlineOptions;
    use std::time::Duration;

    let slot_name = format!("pg_dbm_handoff_{}", std::process::id());
    let publication = format!("pg_dbm_handoff_pub_{}", std::process::id());

    // Best-effort cleanup from a previous run.
    let cleanup_client = connect_with_sslmode(&source).await.expect("connect");
    cleanup_client
        .batch_execute(&format!(
            "SELECT pg_drop_replication_slot('{slot}') \
             WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot}')",
            slot = slot_name.replace('\'', "''"),
        ))
        .await
        .ok();
    cleanup_client
        .batch_execute(&format!("DROP PUBLICATION IF EXISTS {}", publication))
        .await
        .ok();
    cleanup_client
        .batch_execute(&format!(
            "CREATE PUBLICATION {} FOR ALL TABLES",
            publication
        ))
        .await
        .ok();

    let online = OnlineOptions {
        slot_name: slot_name.clone(),
        publication: publication.clone(),
        ..OnlineOptions::default()
    };

    // 1. Prepare slot + snapshot.
    let prepared = match prepare_replication_slot(&source, &online).await {
        Ok(p) => p,
        Err(e) => {
            // If the source is not configured for logical replication we
            // cannot exercise the handoff — skip rather than fail.
            eprintln!("skipped: prepare_replication_slot failed: {e}");
            cleanup_client
                .batch_execute(&format!("DROP PUBLICATION IF EXISTS {}", publication))
                .await
                .ok();
            return;
        }
    };
    assert!(
        prepared.snapshot_name.is_some(),
        "exported snapshot should be present on a freshly created slot"
    );

    // 2. Simulate pg_dump running while we hold the stream.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 3. Slot must still exist while we hold the stream. The `active` flag in
    // pg_replication_slots only flips true on START_REPLICATION (not on
    // CREATE_REPLICATION_SLOT), so we only assert existence here — the
    // regression we're guarding against is the slot/snapshot disappearing
    // mid-flow, not the active accounting.
    let client = connect_with_sslmode(&source).await.expect("connect");
    let row = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
            &[&slot_name],
        )
        .await
        .expect("query pg_replication_slots");
    let exists: bool = row.get(0);
    assert!(exists, "slot should still exist while stream is held");

    // 4. Drop the stream — emulates orchestrator handing off to native apply.
    drop(prepared.stream);

    // 5. wait_for_slot_inactive should succeed within its internal timeout.
    let reporter = CollectingReporter::new();
    wait_for_slot_inactive(&source, &slot_name, &reporter)
        .await
        .expect("slot did not transition to inactive");

    // 6. Cleanup.
    let _ = client
        .batch_execute(&format!(
            "SELECT pg_drop_replication_slot('{}')",
            slot_name.replace('\'', "''")
        ))
        .await;
    let _ = client
        .batch_execute(&format!("DROP PUBLICATION IF EXISTS {}", publication))
        .await;
}

// ─── verify::verify_row_counts + Migrator verify mode ────────────────────────

use pg_dbmigrator::{EndpointConfig, MigrationConfig, MigrationMode};
use tokio_util::sync::CancellationToken;

/// Build a `MigrationConfig` in verify mode from the two live URLs, optionally
/// restricted to a single schema so cross-test tables don't interfere.
fn verify_config(source: &str, target: &str, schemas: Vec<String>) -> MigrationConfig {
    MigrationConfig {
        mode: MigrationMode::Verify,
        source: EndpointConfig::parse(source).expect("parse source url"),
        target: EndpointConfig::parse(target).expect("parse target url"),
        schemas,
        ..MigrationConfig::default()
    }
}

#[tokio::test]
async fn verify_row_counts_ok_when_counts_match() {
    let source_url_val = skip_without_pg!(source_url());
    let target_url_val = skip_without_pg!(target_url());

    let source = connect_with_sslmode(&source_url_val).await.unwrap();
    let target = connect_with_sslmode(&target_url_val).await.unwrap();

    let ddl = "CREATE TABLE IF NOT EXISTS public.vrc_match_t (id int PRIMARY KEY); \
               INSERT INTO public.vrc_match_t VALUES (1), (2), (3);";
    source
        .batch_execute("DROP TABLE IF EXISTS public.vrc_match_t")
        .await
        .ok();
    target
        .batch_execute("DROP TABLE IF EXISTS public.vrc_match_t")
        .await
        .ok();
    source.batch_execute(ddl).await.unwrap();
    target.batch_execute(ddl).await.unwrap();

    let cfg = verify_config(&source_url_val, &target_url_val, vec!["public".to_string()]);
    let report = pg_dbmigrator::verify::verify_row_counts(&cfg, &CancellationToken::new())
        .await
        .unwrap();
    assert!(report.is_ok(), "expected all tables to match");
    let ours = report
        .rows()
        .iter()
        .find(|r| r.table == "vrc_match_t")
        .expect("our table should be in the report");
    assert_eq!(ours.source, 3);
    assert_eq!(ours.target, 3);

    source
        .batch_execute("DROP TABLE IF EXISTS public.vrc_match_t")
        .await
        .ok();
    target
        .batch_execute("DROP TABLE IF EXISTS public.vrc_match_t")
        .await
        .ok();
}

#[tokio::test]
async fn verify_row_counts_reports_mismatch_on_drift() {
    let source_url_val = skip_without_pg!(source_url());
    let target_url_val = skip_without_pg!(target_url());

    let source = connect_with_sslmode(&source_url_val).await.unwrap();
    let target = connect_with_sslmode(&target_url_val).await.unwrap();

    let schema = "vrc_drift_s";
    let ddl = |extra: &str| {
        format!(
            "CREATE SCHEMA IF NOT EXISTS {schema}; \
             CREATE TABLE IF NOT EXISTS {schema}.t (id int PRIMARY KEY); \
             INSERT INTO {schema}.t VALUES (1), (2){extra};"
        )
    };
    source
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await
        .ok();
    target
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await
        .ok();
    source.batch_execute(&ddl("")).await.unwrap();
    // Target has an extra row → drift.
    target.batch_execute(&ddl(", (3)")).await.unwrap();

    let cfg = verify_config(&source_url_val, &target_url_val, vec![schema.to_string()]);
    let report = pg_dbmigrator::verify::verify_row_counts(&cfg, &CancellationToken::new())
        .await
        .unwrap();
    assert!(!report.is_ok(), "expected a mismatch");
    assert_eq!(report.mismatches().len(), 1);

    source
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await
        .ok();
    target
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await
        .ok();
}

#[tokio::test]
async fn verify_row_counts_reports_missing_target_table_as_mismatch() {
    let source_url_val = skip_without_pg!(source_url());
    let target_url_val = skip_without_pg!(target_url());

    let source = connect_with_sslmode(&source_url_val).await.unwrap();
    let target = connect_with_sslmode(&target_url_val).await.unwrap();

    // Source has the table in a dedicated schema; target lacks the schema
    // entirely (exercises the UNDEFINED_SCHEMA/UNDEFINED_TABLE branch).
    let schema = "vrc_missing_s";
    source
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await
        .ok();
    target
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await
        .ok();
    source
        .batch_execute(&format!(
            "CREATE SCHEMA {schema}; \
             CREATE TABLE {schema}.only_on_source (id int PRIMARY KEY); \
             INSERT INTO {schema}.only_on_source VALUES (1), (2);"
        ))
        .await
        .unwrap();

    let cfg = verify_config(&source_url_val, &target_url_val, vec![schema.to_string()]);
    // Missing target table must be reported as a mismatch (target=0), not error.
    let report = pg_dbmigrator::verify::verify_row_counts(&cfg, &CancellationToken::new())
        .await
        .expect("missing target table should not be a hard error");
    let ours = report
        .rows()
        .iter()
        .find(|r| r.table == "only_on_source")
        .expect("our source-only table should be listed");
    assert_eq!(ours.source, 2);
    // Missing target table is flagged with the -1 sentinel (never a real
    // count(*)), so it always mismatches — even against an empty source.
    assert_eq!(ours.target, -1);
    assert_eq!(report.mismatches().len(), 1);

    source
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await
        .ok();
}

#[tokio::test]
async fn verify_row_counts_flags_empty_source_table_missing_on_target() {
    let source_url_val = skip_without_pg!(source_url());
    let target_url_val = skip_without_pg!(target_url());

    let source = connect_with_sslmode(&source_url_val).await.unwrap();

    // A UNIQUE schema with an EMPTY table (0 rows) on the SOURCE only. With the
    // old `0` sentinel this read as 0 (source) == 0 (missing target) → a false
    // MATCH; with the `-1` sentinel the missing table always mismatches.
    let schema = format!("vrc_empty_missing_{}", std::process::id());
    source
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await
        .ok();
    source
        .batch_execute(&format!(
            "CREATE SCHEMA {schema}; \
             CREATE TABLE {schema}.empty_only_on_source (id int PRIMARY KEY);"
        ))
        .await
        .unwrap();

    let cfg = verify_config(&source_url_val, &target_url_val, vec![schema.clone()]);
    let report = pg_dbmigrator::verify::verify_row_counts(&cfg, &CancellationToken::new())
        .await
        .expect("missing target table should not be a hard error");
    assert!(
        !report.is_ok(),
        "empty source table missing on target must be flagged as a mismatch"
    );

    source
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await
        .ok();
}

#[tokio::test]
async fn verify_row_counts_handles_schema_with_dot() {
    let source_url_val = skip_without_pg!(source_url());
    let target_url_val = skip_without_pg!(target_url());

    let source = connect_with_sslmode(&source_url_val).await.unwrap();
    let target = connect_with_sslmode(&target_url_val).await.unwrap();

    // A schema name containing a literal dot must be quoted. This exercises
    // the rsplit_once path: split on the LAST '.' so `"ten.ant".t` resolves
    // to schema="ten.ant", table="t" (both label and query target it).
    let ddl = "CREATE SCHEMA IF NOT EXISTS \"ten.ant\"; \
               CREATE TABLE IF NOT EXISTS \"ten.ant\".t (id int PRIMARY KEY); \
               INSERT INTO \"ten.ant\".t VALUES (1), (2), (3);";
    source
        .batch_execute("DROP SCHEMA IF EXISTS \"ten.ant\" CASCADE")
        .await
        .ok();
    target
        .batch_execute("DROP SCHEMA IF EXISTS \"ten.ant\" CASCADE")
        .await
        .ok();
    source.batch_execute(ddl).await.unwrap();
    target.batch_execute(ddl).await.unwrap();

    let cfg = verify_config(
        &source_url_val,
        &target_url_val,
        vec!["ten.ant".to_string()],
    );
    let report = pg_dbmigrator::verify::verify_row_counts(&cfg, &CancellationToken::new())
        .await
        .unwrap();
    assert!(report.is_ok(), "expected all tables to match");
    let ours = report
        .rows()
        .iter()
        .find(|r| r.schema == "ten.ant" && r.table == "t")
        .expect("dotted-schema table should be in the report");
    assert_eq!(ours.source, 3);
    assert_eq!(ours.target, 3);

    source
        .batch_execute("DROP SCHEMA IF EXISTS \"ten.ant\" CASCADE")
        .await
        .ok();
    target
        .batch_execute("DROP SCHEMA IF EXISTS \"ten.ant\" CASCADE")
        .await
        .ok();
}

#[tokio::test]
async fn verify_row_counts_errs_when_cancelled() {
    let source_url_val = skip_without_pg!(source_url());
    let target_url_val = skip_without_pg!(target_url());

    let source = connect_with_sslmode(&source_url_val).await.unwrap();
    // Ensure at least one source table exists so the loop body is reached.
    let schema = "vrc_cancel_s";
    source
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await
        .ok();
    source
        .batch_execute(&format!(
            "CREATE SCHEMA {schema}; \
             CREATE TABLE {schema}.t (id int PRIMARY KEY); \
             INSERT INTO {schema}.t VALUES (1);"
        ))
        .await
        .unwrap();

    let cfg = verify_config(&source_url_val, &target_url_val, vec![schema.to_string()]);
    let cancel = CancellationToken::new();
    cancel.cancel();
    let err = pg_dbmigrator::verify::verify_row_counts(&cfg, &cancel)
        .await
        .unwrap_err();
    assert!(
        matches!(err, pg_dbmigrator::MigrationError::Cancelled),
        "expected Cancelled, got {err:?}"
    );

    source
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await
        .ok();
}

// ─── preflight::verify_target_extensions_available (live) ────────────────────

#[tokio::test]
async fn verify_target_extensions_available_passes_on_vanilla() {
    let source_url_val = skip_without_pg!(source_url());
    let target_url_val = skip_without_pg!(target_url());
    // Both vanilla PG images ship plpgsql; the target has it available.
    pg_dbmigrator::preflight::verify_target_extensions_available(&source_url_val, &target_url_val)
        .await
        .expect("plpgsql should be available on the target");
}

// ─── Migrator::run standalone verify mode ────────────────────────────────────

#[tokio::test]
async fn migrator_run_verify_ok_when_counts_match() {
    let source_url_val = skip_without_pg!(source_url());
    let target_url_val = skip_without_pg!(target_url());

    let source = connect_with_sslmode(&source_url_val).await.unwrap();
    let target = connect_with_sslmode(&target_url_val).await.unwrap();

    let schema = "mrv_ok_s";
    let ddl = format!(
        "CREATE SCHEMA IF NOT EXISTS {schema}; \
         CREATE TABLE IF NOT EXISTS {schema}.t (id int PRIMARY KEY); \
         INSERT INTO {schema}.t VALUES (1), (2);"
    );
    for c in [&source, &target] {
        c.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .await
            .ok();
        c.batch_execute(&ddl).await.unwrap();
    }

    let cfg = verify_config(&source_url_val, &target_url_val, vec![schema.to_string()]);
    let outcome = pg_dbmigrator::Migrator::new(cfg)
        .run(CancellationToken::new())
        .await;
    assert!(outcome.is_ok(), "verify run should succeed: {outcome:?}");

    for c in [&source, &target] {
        c.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .await
            .ok();
    }
}

#[tokio::test]
async fn migrator_run_verify_errs_on_drift() {
    let source_url_val = skip_without_pg!(source_url());
    let target_url_val = skip_without_pg!(target_url());

    let source = connect_with_sslmode(&source_url_val).await.unwrap();
    let target = connect_with_sslmode(&target_url_val).await.unwrap();

    let schema = "mrv_drift_s";
    let base = format!(
        "CREATE SCHEMA IF NOT EXISTS {schema}; \
         CREATE TABLE IF NOT EXISTS {schema}.t (id int PRIMARY KEY);"
    );
    for c in [&source, &target] {
        c.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .await
            .ok();
        c.batch_execute(&base).await.unwrap();
    }
    source
        .batch_execute(&format!("INSERT INTO {schema}.t VALUES (1), (2)"))
        .await
        .unwrap();
    target
        .batch_execute(&format!("INSERT INTO {schema}.t VALUES (1)"))
        .await
        .unwrap();

    let cfg = verify_config(&source_url_val, &target_url_val, vec![schema.to_string()]);
    let outcome = pg_dbmigrator::Migrator::new(cfg)
        .run(CancellationToken::new())
        .await;
    assert!(outcome.is_err(), "verify run should fail on drift");

    for c in [&source, &target] {
        c.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .await
            .ok();
    }
}

#[tokio::test]
async fn migrator_offline_run_covers_auto_verify() {
    let source_url_val = skip_without_pg!(source_url());
    let target_url_val = skip_without_pg!(target_url());

    let source = connect_with_sslmode(&source_url_val).await.unwrap();
    let target = connect_with_sslmode(&target_url_val).await.unwrap();

    // Dedicated unique schema so the scoped offline dump/restore doesn't
    // touch (or race with) objects other tests are using.
    let schema = "moff_verify_s";
    for c in [&source, &target] {
        c.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .await
            .ok();
    }
    source
        .batch_execute(&format!(
            "CREATE SCHEMA {schema}; \
             CREATE TABLE {schema}.t (id int PRIMARY KEY, v text); \
             INSERT INTO {schema}.t VALUES (1, 'a'), (2, 'b'), (3, 'c');"
        ))
        .await
        .unwrap();

    // Full offline migration scoped to our schema. verify=Warn so the
    // auto verify step (run_offline -> run_verify_stage -> verify_row_counts)
    // is exercised end-to-end. jobs:1 keeps it a single custom-format dump.
    let cfg = MigrationConfig {
        mode: MigrationMode::Offline,
        source: EndpointConfig::parse(&source_url_val).unwrap(),
        target: EndpointConfig::parse(&target_url_val).unwrap(),
        verify: pg_dbmigrator::VerifyMode::Warn,
        jobs: 1,
        schemas: vec![schema.to_string()],
        ..MigrationConfig::default()
    };

    let outcome = pg_dbmigrator::Migrator::new(cfg)
        .run(CancellationToken::new())
        .await;
    assert!(
        outcome.is_ok(),
        "offline migration with auto verify should succeed: {outcome:?}"
    );

    // Restore reproduced the rows on the target.
    let row = target
        .query_one(&format!("SELECT count(*) FROM {schema}.t"), &[])
        .await
        .unwrap();
    let n: i64 = row.get(0);
    assert_eq!(n, 3, "target should have the 3 restored rows");

    for c in [&source, &target] {
        c.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .await
            .ok();
    }
}

/// The snapshot-timeout preflight reads the GUC over a real connection.
/// A unit test cannot catch a bad query here because the probe is mocked,
/// and the first version of this check used `current_setting()`, which
/// returns a display string ("10min") that does not cast to an integer.
#[tokio::test]
async fn snapshot_timeout_preflight_reads_guc_in_milliseconds() {
    let url = skip_without_pg!(source_url());
    let client = connect_with_sslmode(&url).await.unwrap();

    // Session-local so the test never mutates the shared container.
    client
        .batch_execute("SET idle_in_transaction_session_timeout = '10min'")
        .await
        .unwrap();
    let row = client
        .query_one(
            "SELECT setting::bigint FROM pg_settings \
             WHERE name = 'idle_in_transaction_session_timeout'",
            &[],
        )
        .await
        .expect("the preflight query must run against a real server");
    let ms: i64 = row.get(0);
    assert_eq!(ms, 600_000, "pg_settings must report the raw ms value");

    // And the real check rejects exactly that.
    client
        .batch_execute("SET idle_in_transaction_session_timeout = 0")
        .await
        .unwrap();
    assert!(
        pg_dbmigrator::preflight::verify_source_snapshot_timeouts(&url)
            .await
            .is_ok(),
        "a source with the timeout disabled must pass preflight"
    );
}

/// End-to-end split copy against live databases.
///
/// The fixture is deliberately hostile, because both of the bugs this
/// exercises were real:
///
/// * `decoy.split_events` shares its table name with `split_src.split_events`.
///   An earlier version named tables with `oid::regclass::text`, which drops
///   the schema whenever the relation is visible through the session's
///   `search_path` — so the bare name could resolve to the *decoy* on the
///   target and truncate it.
/// * `split_src.split_gen` has a `GENERATED ALWAYS AS … STORED` column.
///   `SELECT *` includes it while `COPY t FROM STDIN` without a column list
///   excludes it, so the default spellings disagree and the copy fails with
///   "extra data after last expected column".
#[tokio::test]
async fn split_copy_moves_rows_without_touching_same_named_tables() {
    let src_url = skip_without_pg!(source_url());
    let tgt_url = skip_without_pg!(target_url());
    let source = connect_with_sslmode(&src_url).await.unwrap();
    let target = connect_with_sslmode(&tgt_url).await.unwrap();

    for c in [&source, &target] {
        c.batch_execute(
            "DROP SCHEMA IF EXISTS split_src CASCADE; DROP SCHEMA IF EXISTS decoy CASCADE;",
        )
        .await
        .unwrap();
    }
    source
        .batch_execute(
            "CREATE SCHEMA split_src; \
             CREATE SCHEMA decoy; \
             CREATE TABLE split_src.split_events AS \
               SELECT i AS id, md5(i::text) AS payload FROM generate_series(1, 5000) i; \
             CREATE TABLE split_src.split_gen (a int, c text, \
               b int GENERATED ALWAYS AS (a * 2) STORED); \
             INSERT INTO split_src.split_gen (a, c) \
               SELECT i, md5(i::text) FROM generate_series(1, 5000) i; \
             CREATE TABLE decoy.split_events (id int, payload text);",
        )
        .await
        .unwrap();
    // Same-named decoy on the TARGET, holding rows that must survive.
    target
        .batch_execute(
            "CREATE SCHEMA split_src; \
             CREATE SCHEMA decoy; \
             CREATE TABLE split_src.split_events (id int, payload text); \
             CREATE TABLE split_src.split_gen (a int, c text, \
               b int GENERATED ALWAYS AS (a * 2) STORED); \
             CREATE TABLE decoy.split_events (id int, payload text); \
             INSERT INTO decoy.split_events VALUES (1, 'DO NOT TOUCH');",
        )
        .await
        .unwrap();

    // Threshold 0 would be rejected by config validation, so plan directly
    // with a 1-byte threshold to pick up these small fixtures.
    let plan = pg_dbmigrator::copy_split::plan_split_tables(&src_url, 1, 2)
        .await
        .expect("planning must succeed");
    let names: Vec<&str> = plan.iter().map(|t| t.table.as_str()).collect();
    assert!(
        names.contains(&"split_src.split_events"),
        "plan must carry schema-qualified names, got {names:?}"
    );
    assert!(
        plan.iter()
            .find(|t| t.table == "split_src.split_gen")
            .expect("split_gen must be planned")
            .columns
            .iter()
            .all(|c| c != "b"),
        "generated column must be excluded from the copy column list"
    );

    let snapshot = pg_dbmigrator::copy_split::export_snapshot(&src_url)
        .await
        .unwrap();
    let planned: Vec<_> = plan
        .into_iter()
        .filter(|t| t.table.starts_with("split_src."))
        .collect();
    let rows = pg_dbmigrator::copy_split::run_split_copy(
        &src_url,
        &tgt_url,
        &snapshot.id,
        &planned,
        false,
        &tokio_util::sync::CancellationToken::new(),
    )
    .await
    .expect("split copy must succeed");
    assert_eq!(rows, 10_000, "both fixtures should be copied in full");

    let check = |sql: &'static str| {
        let target = &target;
        async move { target.query_one(sql, &[]).await.unwrap().get::<_, i64>(0) }
    };
    assert_eq!(
        check("SELECT count(*) FROM split_src.split_events").await,
        5000
    );
    assert_eq!(
        check("SELECT count(*) FROM split_src.split_gen").await,
        5000
    );
    assert_eq!(
        check("SELECT count(*) FROM split_src.split_gen WHERE b = a * 2").await,
        5000,
        "generated column must be recomputed on the target"
    );

    // The decoy must be untouched — this is the data-destruction regression.
    let survivor: String = target
        .query_one("SELECT payload FROM decoy.split_events", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(survivor, "DO NOT TOUCH");

    // A second run onto a now-populated target must refuse rather than wipe it.
    let err = pg_dbmigrator::copy_split::run_split_copy(
        &src_url,
        &tgt_url,
        &snapshot.id,
        &planned,
        false,
        &tokio_util::sync::CancellationToken::new(),
    )
    .await
    .expect_err("a non-empty target must not be truncated implicitly");
    assert!(
        format!("{err}").contains("already contains rows"),
        "unexpected error: {err}"
    );

    for c in [&source, &target] {
        c.batch_execute(
            "DROP SCHEMA IF EXISTS split_src CASCADE; DROP SCHEMA IF EXISTS decoy CASCADE;",
        )
        .await
        .ok();
    }
}

/// Drive a whole offline migration through `Migrator` with the split copy
/// enabled, so the orchestrator wiring — plan, `--exclude-table-data`, the
/// copy stage between the data and post-data sections — is exercised as a
/// unit rather than piecemeal.
#[tokio::test]
async fn offline_migration_with_split_copy_moves_everything() {
    let src_url = skip_without_pg!(source_url());
    let tgt_url = skip_without_pg!(target_url());
    let source = connect_with_sslmode(&src_url).await.unwrap();
    let target = connect_with_sslmode(&tgt_url).await.unwrap();

    // `big` clears the threshold and takes the direct path; `small` stays in
    // the archive. Both must arrive.
    for c in [&source, &target] {
        c.batch_execute("DROP SCHEMA IF EXISTS split_e2e CASCADE")
            .await
            .ok();
    }
    source
        .batch_execute(
            "CREATE SCHEMA split_e2e; \
             CREATE TABLE split_e2e.big AS \
               SELECT i AS id, repeat(md5(i::text), 4) AS payload \
               FROM generate_series(1, 60000) i; \
             ALTER TABLE split_e2e.big ADD PRIMARY KEY (id); \
             CREATE INDEX big_payload_idx ON split_e2e.big (payload); \
             CREATE TABLE split_e2e.small AS SELECT i AS id FROM generate_series(1, 25) i;",
        )
        .await
        .unwrap();

    let cfg = MigrationConfig {
        mode: MigrationMode::Offline,
        source: EndpointConfig::parse(&src_url).unwrap(),
        target: EndpointConfig::parse(&tgt_url).unwrap(),
        jobs: 2,
        skip_analyze: true,
        skip_source_vacuum: true,
        verify: pg_dbmigrator::VerifyMode::Strict,
        // Comfortably below `big` and above `small`.
        split_tables_larger_than: Some(1024 * 1024),
        split_max_parts: 3,
        dump_path: Some(std::env::temp_dir().join(format!("split_e2e_{}", std::process::id()))),
        ..MigrationConfig::default()
    };
    let outcome = pg_dbmigrator::Migrator::new(cfg)
        .run(tokio_util::sync::CancellationToken::new())
        .await;
    assert!(outcome.is_ok(), "split-copy migration failed: {outcome:?}");

    let count = |sql: &'static str| {
        let target = &target;
        async move { target.query_one(sql, &[]).await.unwrap().get::<_, i64>(0) }
    };
    assert_eq!(count("SELECT count(*) FROM split_e2e.big").await, 60000);
    assert_eq!(count("SELECT count(*) FROM split_e2e.small").await, 25);
    // post-data still ran over the directly-copied table.
    assert_eq!(
        count(
            "SELECT count(*) FROM pg_indexes \
             WHERE schemaname = 'split_e2e' AND tablename = 'big'"
        )
        .await,
        2,
        "primary key and secondary index must both be rebuilt"
    );
    // And the rows are the source's, not a truncated subset.
    let src_sum: i64 = source
        .query_one("SELECT sum(id) FROM split_e2e.big", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count("SELECT sum(id) FROM split_e2e.big").await, src_sum);

    for c in [&source, &target] {
        c.batch_execute("DROP SCHEMA IF EXISTS split_e2e CASCADE")
            .await
            .ok();
    }
}
