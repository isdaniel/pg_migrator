//! Snapshot / replication-slot management for online migrations.
//!
//! This module is a thin orchestration layer on top of
//! [`pg_walstream::LogicalReplicationStream`]. Its job is to:
//!
//! 1. open a replication connection to the source,
//! 2. create the logical replication slot with `EXPORT_SNAPSHOT`,
//! 3. expose the resulting snapshot id (so that `pg_dump --snapshot=...` can
//!    obtain a consistent view of the database at the slot's start LSN).
//!
//! The actual `START_REPLICATION` is deferred — the orchestrator hands
//! the slot to [`crate::native_apply`] only **after** `pg_dump` has
//! finished, because issuing `START_REPLICATION` invalidates the
//! exported snapshot.

use std::time::Duration;

use pg_walstream::{
    LogicalReplicationStream, ReplicationSlotOptions, ReplicationStreamConfig, RetryConfig,
    StreamingMode,
};
use tracing::{info, warn};

use crate::config::OnlineOptions;
use crate::error::Result;

/// Result of [`prepare_replication_slot`].
///
/// Note: [`pg_walstream::LogicalReplicationStream`] does not implement
/// [`Debug`], so this struct cannot derive `Debug` either.
#[allow(missing_debug_implementations)]
pub struct PreparedSlot {
    /// The replication stream with the slot already created. The
    /// orchestrator holds it across the `pg_dump` step (so the exported
    /// snapshot stays alive) and drops it before handing the slot to
    /// [`crate::native_apply::run_native_apply`].
    pub stream: LogicalReplicationStream,
    /// The exported snapshot id, if PostgreSQL returned one. Use this with
    /// `pg_dump --snapshot=<id>` to obtain a consistent dump aligned with the
    /// slot's start LSN.
    pub snapshot_name: Option<String>,
}

/// Build a [`ReplicationStreamConfig`] for the given online options.
///
/// Exposed as `pub` so tests (and callers that want full control) can build
/// the stream themselves.
pub fn build_stream_config(opts: &OnlineOptions) -> ReplicationStreamConfig {
    ReplicationStreamConfig::new(
        opts.slot_name.clone(),
        opts.publication.clone(),
        opts.protocol_version,
        StreamingMode::On,
        opts.apply.feedback_interval,
        opts.apply.connection_timeout,
        opts.apply.health_check_interval,
        RetryConfig::default(),
    )
    .with_slot_options(ReplicationSlotOptions {
        snapshot: Some("export".to_string()),
        ..Default::default()
    })
}

/// Open a replication connection and create the replication slot, returning
/// both the stream and the snapshot name exported by PostgreSQL.
///
/// `connection_string` must include `?replication=database` for libpq to
/// open a replication connection.
pub async fn prepare_replication_slot(
    connection_string: &str,
    opts: &OnlineOptions,
) -> Result<PreparedSlot> {
    info!(slot = %opts.slot_name, publication = %opts.publication, "preparing replication slot");
    opts.validate()?;

    let conn = with_snapshot_keepalives(&ensure_replication_qs(connection_string));
    let cfg = build_stream_config(opts);
    let mut stream = LogicalReplicationStream::new(&conn, cfg).await?;
    stream.ensure_replication_slot().await?;
    let snapshot_name = stream.exported_snapshot_name().map(|s| s.to_string());
    if snapshot_name.is_none() {
        warn!("replication slot was reused — no exported snapshot is available");
    } else {
        info!(?snapshot_name, "exported snapshot ready");
    }
    Ok(PreparedSlot {
        stream,
        snapshot_name,
    })
}

/// Append `replication=database` to a libpq URI if it isn't already present.
///
/// Public so it can be unit-tested independently.
pub fn ensure_replication_qs(connection_string: &str) -> String {
    if connection_string.contains("replication=") {
        return connection_string.to_string();
    }
    if connection_string.contains('?') {
        format!("{connection_string}&replication=database")
    } else {
        format!("{connection_string}?replication=database")
    }
}

/// Bound used by the orchestrator when waiting for the slot to be ready.
/// Kept here so tests do not need to depend on real timings.
pub const DEFAULT_SLOT_TIMEOUT: Duration = Duration::from_secs(60);

/// TCP keepalive probes for connections that hold a snapshot open.
///
/// Restricted to the three keys **both** connection-string parsers in this
/// workspace accept. `pg_walstream` has its own conninfo parser
/// (`connection/native/conninfo.rs`) that knows `keepalives_count` but not
/// `keepalives_retries`; `tokio-postgres` is the reverse
/// (`config.rs:646-673`), and rejects the whole string with "invalid
/// connection string" on an unknown key. Naming either spelling here makes
/// the result valid for exactly one of the two callers, so neither is named
/// and each parser applies its own retry-count default.
///
/// Note also that `pg_walstream` silently *ignores* conninfo keys it does
/// not know — including libpq's `options` — so session GUCs cannot be
/// pushed down this way. The source-side
/// `idle_in_transaction_session_timeout` is checked in
/// [`crate::preflight::verify_source_snapshot_timeouts`] instead.
const SNAPSHOT_KEEPALIVE_PARAMS: &str = "keepalives=1&keepalives_idle=60&keepalives_interval=10";

/// Turn on TCP keepalives for the connection that creates the replication
/// slot.
///
/// This connection holds the exported snapshot open for the *entire*
/// `pg_dump` — minutes on a small database, hours on a large one — and it
/// sits idle the whole time. Without keepalives a silently dropped peer
/// (NAT timeout, firewall, load balancer) is not noticed until the OS
/// default expires, typically two hours, by which point the dump has
/// already failed against a dead snapshot.
///
/// Caller-supplied values win: if the connection string already mentions
/// `keepalives`, it is left untouched.
pub fn with_snapshot_keepalives(connection_string: &str) -> String {
    if connection_string.contains("keepalives") {
        return connection_string.to_string();
    }
    let sep = if connection_string.contains('?') {
        '&'
    } else {
        '?'
    };
    format!("{connection_string}{sep}{SNAPSHOT_KEEPALIVE_PARAMS}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ensure_replication_qs_appends_when_missing() {
        let out = ensure_replication_qs("postgresql://u:p@h:5432/db");
        assert_eq!(out, "postgresql://u:p@h:5432/db?replication=database");
    }

    #[test]
    fn ensure_replication_qs_appends_with_existing_query() {
        let out = ensure_replication_qs("postgresql://u@h/db?sslmode=require");
        assert_eq!(
            out,
            "postgresql://u@h/db?sslmode=require&replication=database"
        );
    }

    #[test]
    fn ensure_replication_qs_keeps_existing() {
        let already = "postgresql://u@h/db?replication=database";
        assert_eq!(ensure_replication_qs(already), already);
    }

    #[test]
    fn ensure_replication_qs_keeps_other_replication_value() {
        // `replication=true` should also be detected and not re-appended.
        let s = "postgresql://u@h/db?replication=true";
        assert_eq!(ensure_replication_qs(s), s);
    }

    #[test]
    fn build_stream_config_propagates_options() {
        let opts = OnlineOptions {
            slot_name: "slot".into(),
            publication: "pub".into(),
            protocol_version: 2,
            ..OnlineOptions::default()
        };
        let cfg = build_stream_config(&opts);
        assert_eq!(cfg.slot_name, "slot");
        assert_eq!(cfg.publication_name, "pub");
        assert_eq!(cfg.protocol_version, 2);
    }

    #[test]
    fn build_stream_config_uses_apply_intervals() {
        use std::time::Duration;
        let opts = OnlineOptions {
            apply: crate::config::ReplicationApplyConfig {
                feedback_interval: Duration::from_secs(20),
                connection_timeout: Duration::from_secs(45),
                health_check_interval: Duration::from_secs(120),
                max_runtime_seconds: None,
            },
            ..OnlineOptions::default()
        };
        let cfg = build_stream_config(&opts);
        assert_eq!(cfg.feedback_interval, Duration::from_secs(20));
        assert_eq!(cfg.connection_timeout, Duration::from_secs(45));
        assert_eq!(cfg.health_check_interval, Duration::from_secs(120));
    }

    #[test]
    fn default_slot_timeout_is_60_seconds() {
        assert_eq!(DEFAULT_SLOT_TIMEOUT, std::time::Duration::from_secs(60));
    }

    /// `pg_walstream` parses the conninfo itself and recognises these keys,
    /// so assert the exact spelling rather than just "something was added".
    #[test]
    fn with_snapshot_keepalives_appends_recognised_keys() {
        let out = with_snapshot_keepalives("postgresql://u:p@h:5432/db?replication=database");
        assert!(out.starts_with("postgresql://u:p@h:5432/db?replication=database&"));
        for key in [
            "keepalives=1",
            "keepalives_idle=60",
            "keepalives_interval=10",
        ] {
            assert!(out.contains(key), "missing {key} in {out}");
        }
    }

    #[test]
    fn with_snapshot_keepalives_uses_question_mark_when_no_query() {
        let out = with_snapshot_keepalives("postgresql://u@h/db");
        assert_eq!(
            out,
            format!("postgresql://u@h/db?{SNAPSHOT_KEEPALIVE_PARAMS}")
        );
    }

    /// `copy_split::export_snapshot` feeds this string to `tokio-postgres`
    /// while `prepare_replication_slot` feeds it to `pg_walstream`'s own
    /// parser. The two accept different retry-count keys and tokio-postgres
    /// rejects the *entire* string on an unknown one, so the shared value
    /// must stay inside the intersection. Asserting the parse is the only
    /// thing that catches a drift here.
    #[test]
    fn with_snapshot_keepalives_is_accepted_by_tokio_postgres() {
        let out = with_snapshot_keepalives("postgresql://u:p@h:5432/db");
        let cfg: tokio_postgres::Config = out
            .parse()
            .unwrap_or_else(|e| panic!("tokio-postgres rejected {out:?}: {e}"));
        assert!(cfg.get_keepalives());
        assert_eq!(
            cfg.get_keepalives_idle(),
            std::time::Duration::from_secs(60)
        );
        assert!(
            !out.contains("keepalives_count") && !out.contains("keepalives_retries"),
            "retry-count key is parser-specific and must be omitted: {out}"
        );
    }

    #[test]
    fn with_snapshot_keepalives_keeps_caller_supplied_values() {
        let given = "postgresql://u@h/db?keepalives=0";
        assert_eq!(with_snapshot_keepalives(given), given);
    }

    #[test]
    fn with_snapshot_keepalives_is_idempotent() {
        let once = with_snapshot_keepalives("postgresql://u@h/db?replication=database");
        assert_eq!(with_snapshot_keepalives(&once), once);
    }
}
