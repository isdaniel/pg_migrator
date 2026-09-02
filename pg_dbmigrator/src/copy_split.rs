//! Direct source → target parallel `COPY` for tables that `pg_dump` cannot
//! parallelise.
//!
//! `pg_dump --jobs N` parallelises across *archive entries*, and a table is
//! one entry. A single 300 GB table therefore dumps on one worker no matter
//! how high `--jobs` goes, and the bytes additionally land on the migrator's
//! disk before being read back out and pushed to the target — two network
//! hops for data that only needs one.
//!
//! This module takes the tables above a size threshold out of the archive
//! (`pg_dump --exclude-table-data`) and moves them itself: N concurrent
//! `COPY (SELECT … WHERE ctid >= … AND ctid < …) TO STDOUT` streams piped
//! straight into `COPY … FROM STDIN` on the target, all reading the same
//! exported snapshot so the result is consistent with the rest of the dump.
//!
//! Two deliberate choices, both measured rather than assumed:
//!
//! * **Text format, not binary.** Binary `COPY` was 8 % *larger* on the wire
//!   for a representative row shape, and it drags in a class of
//!   binary-type-compatibility failures that the text path simply does not
//!   have.
//! * **A low default part count.** Throughput flattened at four streams
//!   (four parts were within 1 % of eight), because the target's write path
//!   saturates well before its CPU does. More streams mostly add contention.
//!
//! The whole feature is opt-in via
//! [`MigrationConfig::split_tables_larger_than`](crate::config::MigrationConfig::split_tables_larger_than);
//! when it is unset nothing here runs and the dump/restore path is byte-for-byte
//! what it was before.

use futures::{SinkExt, StreamExt};
use tokio_postgres::Client;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::error::{MigrationError, Result};
use crate::tls::connect_with_sslmode;

/// One contiguous `ctid` page range of one table, copied by a single stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TablePart {
    /// Fully-qualified, already-quoted table name (from `oid::regclass`).
    pub table: String,
    /// First heap page in this part, inclusive.
    pub start_page: i64,
    /// One past the last heap page, or `None` for the final part.
    ///
    /// The last part is deliberately left unbounded: the page count is read
    /// before the copy starts, and a table that grew in between would
    /// silently lose its tail rows to an upper bound. An open-ended final
    /// range costs nothing and cannot under-copy.
    pub end_page: Option<i64>,
}

impl TablePart {
    /// The `WHERE` clause selecting exactly this part.
    fn ctid_predicate(&self) -> String {
        match self.end_page {
            Some(end) => format!(
                "ctid >= '({},0)'::tid AND ctid < '({},0)'::tid",
                self.start_page, end
            ),
            None => format!("ctid >= '({},0)'::tid", self.start_page),
        }
    }
}

/// A table selected for split copying, plus its parts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitTable {
    /// Schema-qualified, quoted name — `format('%I.%I', nspname, relname)`.
    pub table: String,
    /// On-disk heap size in bytes at planning time.
    pub bytes: i64,
    /// Quoted column names, generated columns excluded.
    ///
    /// Both sides of the copy need an explicit list. `SELECT *` includes
    /// `GENERATED ALWAYS AS … STORED` columns while `COPY t FROM STDIN`
    /// without a list excludes them, so the default spellings disagree and
    /// the copy dies with "extra data after last expected column".
    pub columns: Vec<String>,
    /// Page ranges, one per concurrent stream.
    pub parts: Vec<TablePart>,
}

impl SplitTable {
    /// Comma-separated column list for the `COPY` statements.
    fn column_list(&self) -> String {
        self.columns.join(", ")
    }
}

/// Divide `pages` into at most `max_parts` contiguous ranges.
///
/// Returns an empty vector for an empty table — there is nothing to
/// parallelise and the caller should fall back to the normal dump path.
/// The final range is always open-ended; see [`TablePart::end_page`].
pub fn split_pages(table: &str, pages: i64, max_parts: usize) -> Vec<TablePart> {
    if pages <= 0 || max_parts == 0 {
        return Vec::new();
    }
    let parts = max_parts.max(1) as i64;
    let step = pages.div_euclid(parts).max(1);
    let mut out = Vec::new();
    let mut start = 0i64;
    while start < pages {
        let end = start + step;
        // Fold a short trailing remainder into the previous part rather than
        // spawning a stream for a handful of pages.
        let is_last = end >= pages || out.len() as i64 == parts - 1;
        out.push(TablePart {
            table: table.to_string(),
            start_page: start,
            end_page: if is_last { None } else { Some(end) },
        });
        if is_last {
            break;
        }
        start = end;
    }
    out
}

/// Find every ordinary table at or above `threshold_bytes` and plan its parts.
///
/// Partitioned parents (`relkind = 'p'`) are skipped: `pg_dump` already
/// parallelises those across their partitions, which are themselves ordinary
/// tables and so are considered individually.
///
/// Names are built with `format('%I.%I', …)`, **not** `oid::regclass::text`.
/// `regclass` omits the schema whenever the relation happens to be visible
/// through the *current* session's `search_path`, which would produce a bare
/// `events` for `migrator.events` under the default `"$user", public`. That
/// bare name is later resolved again on the *target*, whose `search_path`
/// need not agree — so the copy could truncate and overwrite an entirely
/// different table. Always carry the schema.
pub async fn plan_split_tables(
    source_conn: &str,
    threshold_bytes: u64,
    max_parts: usize,
) -> Result<Vec<SplitTable>> {
    let client = connect_with_sslmode(source_conn).await?;
    let rows = client
        .query(
            "SELECT format('%I.%I', n.nspname, c.relname) AS name, \
                    pg_relation_size(c.oid) AS bytes, \
                    (pg_relation_size(c.oid) / current_setting('block_size')::bigint) AS pages, \
                    (SELECT coalesce( \
                              array_agg(quote_ident(a.attname) ORDER BY a.attnum), \
                              '{}') \
                     FROM pg_attribute a \
                     WHERE a.attrelid = c.oid \
                       AND a.attnum > 0 \
                       AND NOT a.attisdropped \
                       AND a.attgenerated = '') AS cols \
             FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relkind = 'r' \
               AND n.nspname NOT IN ('pg_catalog', 'information_schema') \
               AND n.nspname NOT LIKE 'pg_toast%' \
               AND n.nspname NOT LIKE 'pg_temp%' \
               AND pg_relation_size(c.oid) >= $1 \
             ORDER BY bytes DESC",
            &[&(threshold_bytes as i64)],
        )
        .await?;

    let mut plan = Vec::new();
    for row in rows {
        let table: String = row.get("name");
        let bytes: i64 = row.get("bytes");
        let pages: i64 = row.get("pages");
        let columns: Vec<String> = row.get("cols");
        if columns.is_empty() {
            warn!(table = %table, "no copyable columns; leaving this table to pg_dump");
            continue;
        }
        let parts = split_pages(&table, pages, max_parts);
        if parts.is_empty() {
            continue;
        }
        info!(
            table = %table,
            bytes,
            parts = parts.len(),
            "table selected for split copy"
        );
        plan.push(SplitTable {
            table,
            bytes,
            columns,
            parts,
        });
    }
    Ok(plan)
}

/// Session settings every split-copy connection needs.
///
/// A `statement_timeout` or `lock_timeout` inherited from the role or the
/// database — the norm on managed PostgreSQL — will kill a multi-hour
/// `COPY` or block the `TRUNCATE`. `pg_dump` and `pg_restore` disable these
/// on their own connections; this code has to do the same for its own.
/// `idle_in_transaction_session_timeout` matters for the snapshot holder,
/// which sits idle for the whole dump.
const COPY_SESSION_GUCS: &str = "SET statement_timeout = 0; \
                                 SET lock_timeout = 0; \
                                 SET idle_in_transaction_session_timeout = 0";

/// Open a source connection pinned to `snapshot`, so every stream sees the
/// same view of the data as the `pg_dump` that produced the rest of the
/// archive.
async fn connect_at_snapshot(conn: &str, snapshot: &str) -> Result<Client> {
    let client = connect_with_sslmode(conn).await?;
    client
        .batch_execute(&format!(
            "{COPY_SESSION_GUCS}; \
             BEGIN ISOLATION LEVEL REPEATABLE READ; SET TRANSACTION SNAPSHOT {}",
            pg_walstream::quote_literal(snapshot)?
        ))
        .await?;
    Ok(client)
}

/// A source connection holding an exported snapshot open.
///
/// Offline migrations have no replication slot to export a snapshot from,
/// but the split copy still has to agree with `pg_dump` about which rows
/// exist — otherwise the directly-copied tables come from a different
/// point in time than the archived ones. Holding this value keeps the
/// exporting transaction (and therefore the snapshot) alive; dropping it
/// ends both.
///
/// Online migrations already have one: the replication slot exports a
/// snapshot that the orchestrator holds across the dump, and its id should
/// be used instead of calling this.
#[allow(missing_debug_implementations)]
pub struct ExportedSnapshot {
    /// Kept solely so the exporting transaction stays open.
    _client: Client,
    /// The snapshot id, for `pg_dump --snapshot=` and `SET TRANSACTION SNAPSHOT`.
    pub id: String,
}

impl std::fmt::Debug for ExportedSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExportedSnapshot")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

/// Export a snapshot from the source and hold it open until the returned
/// value is dropped.
pub async fn export_snapshot(source_conn: &str) -> Result<ExportedSnapshot> {
    // Same hazard as the replication-slot connection: this one sits idle for
    // the whole dump *and* the whole restore, so a dropped peer must be
    // detected rather than waited out at the OS default.
    let client =
        connect_with_sslmode(&crate::snapshot::with_snapshot_keepalives(source_conn)).await?;
    client
        .batch_execute(&format!(
            "{COPY_SESSION_GUCS}; BEGIN ISOLATION LEVEL REPEATABLE READ"
        ))
        .await?;
    let row = client.query_one("SELECT pg_export_snapshot()", &[]).await?;
    let id: String = row.get(0);
    info!(snapshot = %id, "exported snapshot for split copy");
    Ok(ExportedSnapshot {
        _client: client,
        id,
    })
}

/// Copy a single part. Returns the number of rows written to the target.
///
/// Both statements carry an explicit column list; see
/// [`SplitTable::columns`] for why the defaults cannot be trusted to match.
async fn copy_part(
    source: &Client,
    target: &Client,
    table: &SplitTable,
    part: &TablePart,
) -> Result<u64> {
    let cols = table.column_list();
    let out_sql = format!(
        "COPY (SELECT {cols} FROM {} WHERE {}) TO STDOUT",
        table.table,
        part.ctid_predicate()
    );
    let in_sql = format!("COPY {} ({cols}) FROM STDIN", table.table);

    let reader = source.copy_out(&out_sql).await?;
    let writer = target.copy_in::<_, bytes::Bytes>(&in_sql).await?;
    futures::pin_mut!(reader);
    futures::pin_mut!(writer);

    while let Some(chunk) = reader.next().await {
        writer.send(chunk?).await?;
    }
    let rows = writer.finish().await?;
    Ok(rows)
}

/// Copy every planned part, running each table's parts concurrently.
///
/// Returns the total number of rows copied. Tables are processed one at a
/// time so the configured part count is also the real concurrency ceiling —
/// the target's write path, not its core count, is what saturates.
///
/// `allow_nonempty_target` mirrors `--drop-target-first`: without it, a
/// target table that already holds rows aborts the run instead of being
/// truncated.
pub async fn run_split_copy(
    source_conn: &str,
    target_conn: &str,
    snapshot: &str,
    plan: &[SplitTable],
    allow_nonempty_target: bool,
    cancel: &CancellationToken,
) -> Result<u64> {
    let mut total = 0u64;
    for table in plan {
        if cancel.is_cancelled() {
            return Err(MigrationError::Cancelled);
        }
        let started = std::time::Instant::now();

        // Pre-data has just created this table, so it should be empty. Clear
        // it anyway to make a retried restore stage idempotent instead of
        // doubling every row — but refuse if it already holds data. A
        // non-empty table here means the name resolved to something we did
        // not create, and silently wiping it would be the worst possible
        // outcome. `--drop-target-first` is the explicit opt-in for
        // overwriting a populated target.
        {
            let target = connect_with_sslmode(target_conn).await?;
            target.batch_execute(COPY_SESSION_GUCS).await?;
            if !allow_nonempty_target {
                let occupied = target
                    .query_one(
                        &format!("SELECT EXISTS (SELECT 1 FROM {} LIMIT 1)", table.table),
                        &[],
                    )
                    .await?
                    .get::<_, bool>(0);
                if occupied {
                    return Err(MigrationError::config(format!(
                        "target table {} already contains rows; refusing to truncate it for a \
                         split copy. Pass --drop-target-first to overwrite an existing target, \
                         or drop the table first.",
                        table.table
                    )));
                }
            }
            target
                .batch_execute(&format!("TRUNCATE {}", table.table))
                .await?;
        }

        let workers = table.parts.iter().map(|part| async move {
            let (source, target) = tokio::try_join!(
                connect_at_snapshot(source_conn, snapshot),
                connect_with_sslmode(target_conn),
            )?;
            // Bulk load into a table with no indexes yet: the WAL still
            // records everything, we just stop waiting for each flush.
            target
                .batch_execute(&format!(
                    "{COPY_SESSION_GUCS}; SET synchronous_commit = off"
                ))
                .await?;
            copy_part(&source, &target, table, part).await
        });

        let counts = tokio::select! {
            _ = cancel.cancelled() => return Err(MigrationError::Cancelled),
            res = futures::future::try_join_all(workers) => res?,
        };

        let rows: u64 = counts.iter().sum();
        total += rows;
        info!(
            table = %table.table,
            rows,
            parts = table.parts.len(),
            elapsed_secs = started.elapsed().as_secs_f64(),
            "split copy complete"
        );
        if rows == 0 {
            warn!(table = %table.table, "split copy moved 0 rows — was the table empty?");
        }
    }
    Ok(total)
}

/// Parse a human-friendly size (`1000`, `512MB`, `10G`, `2 TiB`) into bytes.
///
/// Both `MB` and `MiB` mean 1024², matching how PostgreSQL itself reports
/// sizes; there is no decimal-megabyte interpretation to get wrong.
pub fn parse_size(input: &str) -> Result<u64> {
    let s = input.trim();
    let digits_end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (num, suffix) = s.split_at(digits_end);
    if num.is_empty() {
        return Err(MigrationError::config(format!(
            "invalid size {input:?}: expected a number, optionally followed by KB/MB/GB/TB"
        )));
    }
    let value: u64 = num.parse().map_err(|_| {
        MigrationError::config(format!("invalid size {input:?}: {num:?} is out of range"))
    })?;
    let shift = match suffix.trim().to_ascii_uppercase().as_str() {
        "" | "B" => 0,
        "K" | "KB" | "KIB" => 10,
        "M" | "MB" | "MIB" => 20,
        "G" | "GB" | "GIB" => 30,
        "T" | "TB" | "TIB" => 40,
        other => {
            return Err(MigrationError::config(format!(
                "invalid size {input:?}: unknown unit {other:?} (use KB, MB, GB or TB)"
            )))
        }
    };
    value.checked_shl(shift).ok_or_else(|| {
        MigrationError::config(format!("invalid size {input:?}: value overflows 64 bits"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_pages_produces_requested_part_count() {
        let parts = split_pages("t", 1000, 4);
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[0].start_page, 0);
        assert_eq!(parts[0].end_page, Some(250));
        assert_eq!(parts[3].start_page, 750);
    }

    /// The tail must stay open-ended, or rows written to pages past the
    /// planning-time page count would be silently dropped.
    #[test]
    fn split_pages_leaves_last_part_unbounded() {
        for parts in [1usize, 2, 4, 8] {
            let out = split_pages("t", 1000, parts);
            assert_eq!(out.last().unwrap().end_page, None, "parts={parts}");
        }
    }

    #[test]
    fn split_pages_covers_every_page_without_gaps() {
        let parts = split_pages("t", 997, 4);
        assert_eq!(parts[0].start_page, 0);
        for pair in parts.windows(2) {
            assert_eq!(
                pair[0].end_page,
                Some(pair[1].start_page),
                "gap or overlap between {:?} and {:?}",
                pair[0],
                pair[1]
            );
        }
    }

    #[test]
    fn split_pages_handles_tables_smaller_than_part_count() {
        let parts = split_pages("t", 3, 8);
        assert!(!parts.is_empty());
        assert_eq!(parts[0].start_page, 0);
        assert_eq!(parts.last().unwrap().end_page, None);
        assert!(parts.len() <= 3, "got {} parts for 3 pages", parts.len());
    }

    #[test]
    fn split_pages_rejects_empty_or_zero_parts() {
        assert!(split_pages("t", 0, 4).is_empty());
        assert!(split_pages("t", 100, 0).is_empty());
    }

    #[test]
    fn ctid_predicate_is_bounded_except_for_the_tail() {
        let bounded = TablePart {
            table: "t".into(),
            start_page: 10,
            end_page: Some(20),
        };
        assert_eq!(
            bounded.ctid_predicate(),
            "ctid >= '(10,0)'::tid AND ctid < '(20,0)'::tid"
        );
        let tail = TablePart {
            table: "t".into(),
            start_page: 20,
            end_page: None,
        };
        assert_eq!(tail.ctid_predicate(), "ctid >= '(20,0)'::tid");
    }

    #[test]
    fn parse_size_accepts_bare_bytes_and_units() {
        assert_eq!(parse_size("1000").unwrap(), 1000);
        assert_eq!(parse_size("1B").unwrap(), 1);
        assert_eq!(parse_size("1KB").unwrap(), 1024);
        assert_eq!(parse_size("512MB").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_size("10G").unwrap(), 10 * 1024 * 1024 * 1024);
        assert_eq!(parse_size("2TiB").unwrap(), 2u64 << 40);
    }

    #[test]
    fn parse_size_is_case_and_space_insensitive() {
        assert_eq!(parse_size(" 50 gb ").unwrap(), 50 * 1024 * 1024 * 1024);
        assert_eq!(parse_size("50Gb").unwrap(), parse_size("50GB").unwrap());
    }

    #[test]
    fn parse_size_rejects_garbage() {
        for bad in ["", "GB", "12PB", "abc", "-5"] {
            assert!(parse_size(bad).is_err(), "{bad:?} should be rejected");
        }
    }
}
