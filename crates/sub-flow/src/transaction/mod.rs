// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::Catalog;
use reifydb_core::CommitVersion;
use reifydb_engine::StandardCommandTransaction;
use tracing::instrument;

mod pending;
mod range;
mod read;
mod state;
#[cfg(test)]
mod utils;
mod write;

pub use pending::{Pending, PendingWrites};
use reifydb_transaction::multi::StandardQueryTransaction;

/// A transaction wrapper for flow processing with dual-version read semantics.
///
/// # Architecture
///
/// FlowTransaction provides **dual-version reads** critical for stateful flow processing:
/// 1. **Source data** - Read at CDC event version (snapshot isolation)
/// 2. **Flow state** - Read at latest version (state continuity across CDC events)
/// 3. **Isolated writes** - Local PendingWrites buffer returned to caller
///
/// This dual-version approach allows stateful operators (joins, aggregates, distinct) to:
/// - Process source data at a consistent snapshot (the CDC event version)
/// - Access their own state at the latest version to maintain continuity
///
/// # Dual-Version Read Routing
///
/// Reads are automatically routed to the correct query transaction based on key type:
///
/// ```text
/// ┌─────────────────┐
/// │  FlowTransaction│
/// └────────┬────────┘
///          │
///          ├──► primitive_query (at CDC version)
///          │    - Source tables
///          │    - Source views
///          │    - Regular data
///          │
///          └──► state_query (at latest version)
///               - FlowNodeState
///               - FlowNodeInternalState
///               - Stateful operator state
/// ```
///
/// # Why Dual Versions Matter
///
/// ## Example: Join Operator
/// ```ignore
/// // CDC event arrives at version 100
/// let mut flow_txn = FlowTransaction::new(&parent, CommitVersion(100));
///
/// // Join operator processes the event:
/// // 1. Reads source table at version 100 (snapshot)
/// let source_row = flow_txn.get(&source_key)?;
///
/// // 2. Reads join state at LATEST version (e.g., 150)
/// //    This state contains results from ALL previous CDC events
/// let join_state = flow_txn.state_get(node_id, &state_key)?;
///
/// // Without dual versions, join state would be stale at version 100
/// ```
///
/// ## Example: Distinct Operator
/// ```ignore
/// // Maintains a set of seen values across ALL CDC events
/// let seen = flow_txn.state_get(node_id, &value_key)?;
///
/// // If read at CDC version, would "forget" values seen in later events
/// // Dual-version ensures we see ALL distinct values accumulated so far
/// ```
///
/// # Current Usage Pattern
///
/// FlowTransaction is used in worker threads to process CDC batches:
///
/// ```ignore
/// // In flow worker thread
/// let primitive_query = engine.multi().begin_query_at_version(batch.version)?;
/// let state_query = engine.multi().begin_query_at_version(state_version)?;
///
/// let mut txn = FlowTransaction {
///     version: batch.version,
///     pending: PendingWrites::new(),
///     primitive_query,
///     state_query,
///     catalog: catalog.clone(),
/// };
///
/// for change in batch.changes {
///     flow_engine.process(&mut txn, change, flow_id)?;
/// }
///
/// // Extract pending writes to merge into parent transaction
/// let pending = txn.pending;
/// ```
///
/// # Write Path
///
/// All writes (`set`, `remove`) go to the local `pending` buffer:
/// - Reads check pending buffer first, then delegate to query transactions
/// - Pending writes are extracted and applied to parent transaction by caller
///
/// # Thread Safety
///
/// FlowTransaction is Send because all fields are either Copy, owned, or
pub struct FlowTransaction {
	/// CDC event version for snapshot isolation.
	///
	/// This is the version at which the CDC event was generated, NOT the parent transaction version.
	/// Source data reads see the database state as of this CDC version.
	/// This guarantees proper snapshot isolation - the flow processes data as it existed when
	/// the CDC event was created, regardless of concurrent modifications.
	pub(crate) version: CommitVersion,

	/// Local write buffer for pending changes.
	///
	/// Stores all `set()` and `remove()` operations made by this transaction.
	/// Returned to caller for application to parent transaction.
	pub(crate) pending: PendingWrites,

	/// Read-only query transaction for accessing storage primitive data at CDC snapshot version.
	///
	/// Provides snapshot reads at `version`. Used for reading storage primitives tables/views
	/// to ensure consistent view of the data being processed by the flow.
	pub(crate) primitive_query: StandardQueryTransaction,

	/// Read-only query transaction for accessing flow state at latest version.
	///
	/// Reads at the latest committed version. Used for reading flow state
	/// (join tables, distinct values, counters) that must be visible across
	/// all CDC versions to maintain continuity.
	pub(crate) state_query: StandardQueryTransaction,

	/// Catalog for metadata access (cloned from parent, Arc-based so cheap)
	pub(crate) catalog: Catalog,
}

impl FlowTransaction {
	/// Create a new FlowTransaction from a parent transaction at a specific CDC version.
	///
	/// Creates dual query transactions:
	/// - `primitive_query`: Reads at the specified CDC version (snapshot isolation)
	/// - `state_query`: Reads at the latest version (state continuity)
	///
	/// # Parameters
	/// * `parent` - The parent command transaction to derive from
	/// * `version` - The CDC event version for snapshot isolation (NOT parent.version())
	/// * `catalog` - The catalog for metadata access
	#[instrument(name = "flow::transaction::new", level = "debug", skip(parent, catalog), fields(version = version.0))]
	pub fn new(parent: &StandardCommandTransaction, version: CommitVersion, catalog: Catalog) -> Self {
		let mut primitive_query = parent.multi.begin_query().unwrap();
		primitive_query.read_as_of_version_inclusive(version);

		let state_query = parent.multi.begin_query().unwrap();
		Self {
			version,
			pending: PendingWrites::new(),
			primitive_query,
			state_query,
			catalog,
		}
	}

	/// Update the transaction to read at a new version
	pub fn update_version(&mut self, new_version: CommitVersion) {
		self.version = new_version;
		self.primitive_query.read_as_of_version_inclusive(new_version);
	}

	/// Get access to the catalog for reading metadata
	pub(crate) fn catalog(&self) -> &Catalog {
		&self.catalog
	}
}
