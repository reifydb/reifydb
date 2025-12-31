// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::transaction::MaterializedCatalogTransaction;
use reifydb_core::CommitVersion;
use reifydb_engine::StandardCommandTransaction;
use tracing::instrument;

mod commit;
mod iter_range;
mod metrics;
mod pending;
mod read;
mod state;
#[cfg(test)]
mod utils;
mod write;

pub use metrics::FlowTransactionMetrics;
pub use pending::{Pending, PendingWrites};
use reifydb_transaction::multi::StandardQueryTransaction;

/// A transaction wrapper for flow processing with dual-version read semantics.
///
/// # Architecture
///
/// FlowTransaction provides **dual-version reads** critical for stateful flow processing:
/// 1. **Source data** - Read at CDC event version (snapshot isolation)
/// 2. **Flow state** - Read at latest version (state continuity across CDC events)
/// 3. **Isolated writes** - Local PendingWrites buffer merged back to parent at commit
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
/// FlowTransaction is used in the task-based flow architecture where each flow
/// processes CDC batches sequentially:
///
/// ```ignore
/// // In flow coordinator task (one transaction per batch)
/// let mut txn = engine.begin_command()?;
/// let mut flow_txn = FlowTransaction::new(&txn, batch.version);
///
/// for change in batch.changes {
///     // Operators use dual-version reads internally
///     flow_engine.process(&mut flow_txn, change, flow_id)?;
/// }
///
/// // Merge pending writes back to parent
/// flow_txn.commit(&mut txn)?;
/// txn.commit()?;
/// ```
///
/// # Write Path
///
/// All writes (`set`, `remove`) go to the local `pending` buffer:
/// - Writes are NOT visible to the parent transaction until commit
/// - Reads check pending buffer first, then delegate to query transactions
/// - Buffered writes are merged to parent via `commit()`
///
/// # Thread Safety
///
/// FlowTransaction is Send because all fields are either Copy, owned, or
/// Send (StandardQueryTransaction). StandardCommandTransaction is now
/// natively Send + Sync, requiring no special workarounds.
pub struct FlowTransaction {
	/// CDC event version for snapshot isolation.
	///
	/// This is the version at which the CDC event was generated, NOT the parent transaction version.
	/// Source data reads see the database state as of this CDC version.
	/// This guarantees proper snapshot isolation - the flow processes data as it existed when
	/// the CDC event was created, regardless of concurrent modifications.
	version: CommitVersion,

	/// Local write buffer for uncommitted changes.
	///
	/// Stores all `set()` and `remove()` operations made by this transaction.
	/// NOT shared with other FlowTransactions. Changes are invisible until commit().
	pending: PendingWrites,

	/// Performance metrics tracking reads, writes, and other operations.
	metrics: FlowTransactionMetrics,

	/// Read-only query transaction for accessing storage primitive data at CDC snapshot version.
	///
	/// Provides snapshot reads at `version`. Used for reading storage primitives tables/views
	/// to ensure consistent view of the data being processed by the flow.
	primitive_query: StandardQueryTransaction,

	/// Read-only query transaction for accessing flow state at latest version.
	///
	/// Reads at the latest committed version. Used for reading flow state
	/// (join tables, distinct values, counters) that must be visible across
	/// all CDC versions to maintain continuity.
	state_query: StandardQueryTransaction,

	/// Catalog for metadata access (cloned from parent, Arc-based so cheap)
	catalog: reifydb_catalog::MaterializedCatalog,
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
	#[instrument(name = "flow::transaction::new", level = "debug", skip(parent), fields(version = version.0))]
	pub async fn new(parent: &StandardCommandTransaction, version: CommitVersion) -> Self {
		let mut primitive_query = parent.multi.begin_query().await.unwrap();
		primitive_query.read_as_of_version_inclusive(version);

		let state_query = parent.multi.begin_query().await.unwrap();
		Self {
			version,
			pending: PendingWrites::new(),
			metrics: FlowTransactionMetrics::new(),
			primitive_query,
			state_query,
			catalog: parent.catalog().clone(),
		}
	}

	/// Get the version this transaction is reading at
	pub fn version(&self) -> CommitVersion {
		self.version
	}

	/// Update the transaction to read at a new version
	pub async fn update_version(&mut self, new_version: CommitVersion) {
		self.version = new_version;
		self.primitive_query.read_as_of_version_inclusive(new_version);
	}

	/// Get immutable reference to the metrics
	pub fn metrics(&self) -> &FlowTransactionMetrics {
		&self.metrics
	}

	/// Get access to the catalog for reading metadata
	pub(crate) fn catalog(&self) -> &reifydb_catalog::MaterializedCatalog {
		&self.catalog
	}
}
