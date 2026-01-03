// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::MaterializedCatalog;
use reifydb_core::CommitVersion;
use reifydb_engine::StandardCommandTransaction;
use reifydb_transaction::multi::StandardQueryTransaction;
use tracing::instrument;

mod read;
mod state;
mod write;

/// A transaction wrapper for flow processing with dual-version read semantics.
///
/// # Architecture
///
/// FlowTransaction provides **dual-version reads** for flow processing:
/// 1. **Source data** - Read at CDC event version via `primitive_query` (snapshot isolation)
/// 2. **Flow state** - Read/write at latest version via `cmd` (StandardCommandTransaction)
///
/// This dual-version approach allows stateful operators (joins, aggregates, distinct) to:
/// - Process source data at a consistent snapshot (the CDC event version)
/// - Access their own state at the latest version to maintain continuity
///
/// # Read Routing
///
/// ```text
/// ┌─────────────────┐
/// │  FlowTransaction│
/// └────────┬────────┘
///          │
///          ├──► primitive_query (at CDC version)
///          │    - Source tables
///          │    - Source views
///          │
///          └──► cmd (at latest version)
///               - FlowNodeState
///               - FlowNodeInternalState
///               - All writes
/// ```
///
/// # Write Path
///
/// All writes go directly to the `cmd` (StandardCommandTransaction).
/// The caller is responsible for committing the transaction.
pub struct FlowTransaction<'a> {
	/// CDC event version for snapshot isolation of source data reads.
	version: CommitVersion,

	/// The command transaction for writes and flow state reads (latest version).
	cmd: &'a mut StandardCommandTransaction,

	/// Read-only query transaction for source data at CDC snapshot version.
	primitive_query: StandardQueryTransaction,

	/// Catalog for metadata access.
	catalog: MaterializedCatalog,
}

impl<'a> FlowTransaction<'a> {
	/// Create a new FlowTransaction from a command transaction at a specific CDC version.
	///
	/// # Parameters
	/// * `cmd` - The command transaction for writes and flow state reads
	/// * `version` - The CDC event version for snapshot isolation of source data
	/// * `catalog` - The materialized catalog for metadata access
	#[instrument(name = "flow::transaction::new", level = "debug", skip(cmd, catalog), fields(version = version.0))]
	pub async fn new(
		cmd: &'a mut StandardCommandTransaction,
		version: CommitVersion,
		catalog: &MaterializedCatalog,
	) -> Self {
		let mut primitive_query = cmd.multi.begin_query().await.unwrap();
		primitive_query.read_as_of_version_inclusive(version);

		Self {
			version,
			cmd,
			primitive_query,
			catalog: catalog.clone(),
		}
	}

	/// Get the version this transaction is reading source data at
	pub fn version(&self) -> CommitVersion {
		self.version
	}

	/// Get access to the catalog for reading metadata
	pub(crate) fn catalog(&self) -> &MaterializedCatalog {
		&self.catalog
	}
}
