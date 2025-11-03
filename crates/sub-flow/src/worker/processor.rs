// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_engine::StandardCommandTransaction;

use super::UnitsOfWork;
use crate::engine::FlowEngine;

/// Trait for different worker pool implementations
pub trait WorkerPool {
	/// Process a batch of units of work grouped by flow
	///
	/// Each flow's units are ordered by version and must be processed sequentially.
	/// Different flows can be processed in parallel.
	///
	/// # Arguments
	/// * `txn` - Parent transaction for creating FlowTransactions
	/// * `units` - Units of work grouped by flow
	/// * `engine` - Engine for processing flows
	///
	/// # Returns
	/// Ok(()) if all units processed successfully, Err if any failed
	fn process(
		&self,
		txn: &mut StandardCommandTransaction,
		units: UnitsOfWork,
		engine: &FlowEngine,
	) -> crate::Result<()>;

	/// Get the number of worker threads (for monitoring)
	fn worker_count(&self) -> usize;

	/// Get a name for this worker implementation (for logging)
	fn name(&self) -> &str;
}
