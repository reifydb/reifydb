// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::CommitVersion;
use reifydb_engine::StandardCommandTransaction;

mod commit;
mod iter_range;
mod iter_scan;
mod metrics;
mod pending;
mod read;
mod state;
#[cfg(test)]
mod test_utils;
mod write;

pub use metrics::FlowTransactionMetrics;
pub use pending::{Pending, PendingWrites};
use reifydb_core::interface::{MultiVersionQueryTransaction, MultiVersionTransaction};
use reifydb_transaction::multi::StandardQueryTransaction;

/// A transaction wrapper for parallel flow processing with snapshot isolation.
///
/// # Architecture
///
/// FlowTransaction enables parallel processing of independent data flows by providing:
/// 1. **Snapshot reads** - via a wrapped StandardQueryTransaction reading at a fixed version
/// 2. **Isolated writes** - via a local PendingWrites buffer unique to this transaction
/// 3. **Sequential merge** - buffered writes are applied back to parent at commit time
///
/// # Read Path
///
/// All reads go through the wrapped `query` transaction, which provides a consistent
/// snapshot view of the database at `version`. The query transaction is read-only and
/// cannot modify the underlying storage.
///
/// For keys that have been modified locally:
/// - Reads check the `pending` buffer first
/// - If found there (or marked for removal), return the local value
/// - Otherwise, delegate to the `query` transaction for the snapshot value
///
/// # Write Path
///
/// All writes (`set`, `remove`) go to the local `pending` buffer only:
/// - Writes are NOT visible to the parent transaction
/// - Writes are NOT visible to other FlowTransactions
/// - Writes are NOT persisted to storage
///
/// The pending buffer is committed back to the parent transaction via `commit()`.
///
/// # Parallel Processing Pattern
///
/// ```ignore
/// let mut parent = engine.begin_command()?;
///
/// // Create multiple FlowTransactions from shared parent reference
/// // Each uses the CDC event version for proper snapshot isolation
/// let flow_txns: Vec<FlowTransaction> = cdc_events
///     .iter()
///     .map(|cdc| FlowTransaction::new(&parent, cdc.version))
///     .collect();
///
/// // Process in parallel (e.g., using rayon)
/// let results: Vec<FlowTransaction> = flow_txns
///     .into_par_iter()
///     .map(|mut txn| {
///         // Process flow, making reads and writes
///         process_flow(&mut txn)?;
///         Ok(txn)
///     })
///     .collect()?;
///
/// // Sequential merge back to parent
/// for flow_txn in results {
///     flow_txn.commit(&mut parent)?;
/// }
///
/// // Atomic commit of all changes
/// parent.commit()?;
/// ```
///
/// # Thread Safety
///
/// FlowTransaction implements `Send` because:
/// - `version` is Copy
/// - `query` wraps Arc-based multi-version transaction (Send + Sync)
/// - `pending` and `metrics` are owned and not shared
///
/// This allows FlowTransactions to be moved to worker threads for parallel processing.
pub struct FlowTransaction {
	/// CDC event version for snapshot isolation.
	///
	/// This is the version at which the CDC event was generated, NOT the parent transaction version.
	/// All reads through the query transaction see the database state as of this CDC version.
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

	/// Read-only query transaction for accessing multi-version storage.
	///
	/// Provides snapshot reads at `version`. This is the primary read path for data
	/// not in the `pending` buffer. The query transaction is configured at construction
	/// time to read at a specific version and cannot be modified.
	query: StandardQueryTransaction,
}

// SAFETY: FlowTransaction can be sent across threads because:
// - version: CommitVersion is Copy (u64 wrapper)
// - query: StandardQueryTransaction wraps Arc-based multi-version storage (Send + Sync)
// - pending: PendingWrites is a BTreeMap owned by this transaction (Send)
// - metrics: FlowTransactionMetrics contains primitive counters (Send)
//
// This enables parallel flow processing where each FlowTransaction is moved to a
// worker thread via rayon or similar thread pool.
unsafe impl Send for FlowTransaction {}

impl FlowTransaction {
	/// Create a new FlowTransaction from a parent transaction at a specific CDC version
	///
	/// Takes a shared reference to the parent, allowing multiple FlowTransactions
	/// to be created for parallel processing.
	///
	/// # Parameters
	/// * `parent` - The parent command transaction to derive from
	/// * `version` - The CDC event version for snapshot isolation (NOT parent.version())
	pub fn new(parent: &StandardCommandTransaction, version: CommitVersion) -> Self {
		let mut query = parent.multi.begin_query().unwrap();
		query.read_as_of_version_inclusive(version).unwrap();

		Self {
			version,
			pending: PendingWrites::new(),
			metrics: FlowTransactionMetrics::new(),
			query,
		}
	}

	/// Get the version this transaction is reading at
	pub fn version(&self) -> CommitVersion {
		self.version
	}

	/// Get immutable reference to the metrics
	pub fn metrics(&self) -> &FlowTransactionMetrics {
		&self.metrics
	}
}
