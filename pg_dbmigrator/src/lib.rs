//! # pg_dbmigrator
//!
//! A Rust library for migrating PostgreSQL databases between two endpoints.
//!
//! * **Offline** — performs `pg_dump` against the source and `pg_restore` (or
//!   `psql`) against the target. Equivalent to a one-shot dump-and-load.
//! * **Online** — first creates a logical replication slot on the source with
//!   `EXPORT_SNAPSHOT`, runs a snapshot-consistent `pg_dump` / `pg_restore`,
//!   and then issues `CREATE SUBSCRIPTION` on the target so PostgreSQL's
//!   built-in apply worker streams WAL changes from the slot until the
//!   operator triggers cutover.
//!
//! The crate is split into small, single-purpose modules so that it can be
//! consumed both as a library and from the bundled CLI binary.
//!
//! ## High level usage
//!
//! ```no_run
//! use pg_dbmigrator::{MigrationConfig, MigrationMode, Migrator, EndpointConfig};
//! use tokio_util::sync::CancellationToken;
//!
//! # async fn run() -> pg_dbmigrator::Result<()> {
//! let cfg = MigrationConfig {
//!     mode: MigrationMode::Offline,
//!     source: EndpointConfig::parse("postgresql://user:pw@src/db")?,
//!     target: EndpointConfig::parse("postgresql://user:pw@dst/db")?,
//!     ..MigrationConfig::default()
//! };
//!
//! let migrator = Migrator::new(cfg);
//! migrator.run(CancellationToken::new()).await?;
//! # Ok(())
//! # }
//! ```
//!
//! See the `examples/` directory for full programs.

#![deny(rust_2018_idioms)]
#![warn(missing_debug_implementations)]

pub mod analyze;
pub mod config;
pub mod copy_split;
pub mod cutover;
pub mod dump;
pub mod error;
pub mod native_apply;
pub mod orchestrator;
pub mod preflight;
pub mod progress;
pub mod restore;
pub mod resume;
pub mod sequences;
pub mod snapshot;
pub mod tls;
pub mod verify;

pub use config::{
    CutoverConfig, EndpointConfig, MigrationConfig, MigrationMode, OnlineOptions,
    ReplicationApplyConfig, VerifyMode,
};
pub use cutover::{CutoverHandle, LagSampler, Transition};
pub use error::{MigrationError, Result};
pub use orchestrator::{cleanup_source_after_cutover, Migrator};
pub use progress::{JsonReporter, MigrationStage, ProgressEvent, ProgressReporter};
pub use resume::{CompletedStage, ResumeToken, RESUME_SCHEMA_VERSION};
pub use verify::{verify_row_counts, TableCount, VerifyReport};
