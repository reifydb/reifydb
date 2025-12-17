// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crossbeam_channel::bounded;
use reifydb_core::interface::FlowId;
use reifydb_engine::StandardCommandTransaction;
use reifydb_sub_api::{SchedulerService, TaskContext, task_once};
use tracing::{Span, trace_span};

use super::{UnitOfWork, UnitsOfWork, WorkerPool};
use crate::{engine::FlowEngine, transaction::FlowTransaction};

/// Parallel worker pool that uses the sub-worker thread pool for execution
///
/// Each flow's units are submitted as a separate high-priority task to the
/// worker pool. Different flows can execute in parallel, but each flow's
/// units are processed sequentially to maintain version ordering.
pub struct ParallelWorkerPool {
	scheduler: SchedulerService,
}

impl ParallelWorkerPool {
	/// Create a new parallel worker pool
	pub fn new(scheduler: SchedulerService) -> Self {
		Self {
			scheduler,
		}
	}
}

impl WorkerPool for ParallelWorkerPool {
	fn process(
		&self,
		txn: &mut StandardCommandTransaction,
		units: UnitsOfWork,
		engine: &FlowEngine,
	) -> crate::Result<()> {
		if units.is_empty() {
			return Ok(());
		}

		let units_of_work = units.into_inner();
		let mut txns: Vec<(Vec<UnitOfWork>, FlowTransaction)> = Vec::with_capacity(units_of_work.len());

		for flow_units in units_of_work {
			if !flow_units.is_empty() {
				// INVARIANT: Validate that all units in this Vec are for the same flow_id
				let flow_id = flow_units[0].flow_id;
				for unit in &flow_units {
					assert_eq!(
						unit.flow_id, flow_id,
						"INVARIANT VIOLATED: Flow units contain mixed flow_ids - expected {:?}, got {:?}. \
						Each Vec should contain units for exactly one flow.",
						flow_id, unit.flow_id
					);
				}

				let first_version = flow_units[0].version;
				let flow_txn = FlowTransaction::new(txn, first_version);
				txns.push((flow_units, flow_txn));
			}
		}

		// INVARIANT: Validate that no flow_id appears in multiple tasks
		// This is critical to prevent keyspace overlap between parallel FlowTransactions
		{
			use std::collections::HashSet;
			let mut flow_ids_in_tasks = HashSet::new();

			for (flow_units, _) in &txns {
				let flow_id = flow_units[0].flow_id;
				assert!(
					!flow_ids_in_tasks.contains(&flow_id),
					"INVARIANT VIOLATED: flow_id {:?} will be processed by multiple parallel tasks. \
					This will cause keyspace overlap as multiple FlowTransactions write to the same keys.",
					flow_id
				);
				flow_ids_in_tasks.insert(flow_id);
			}
		}

		let (result_tx, result_rx) = bounded(txns.len());

		let _submit_span = trace_span!("flow::submit_tasks", task_count = txns.len()).entered();

		for (_seq, (flow_units, mut flow_txn)) in txns.into_iter().enumerate() {
			let result_tx = result_tx.clone();
			let engine = engine.clone();
			let flow_id = flow_units[0].flow_id;
			let unit_count = flow_units.len();
			let change_count: usize = flow_units.iter().map(|u| u.source_changes.len()).sum();

			// Capture parent span for context propagation to worker thread
			let parent_span = Span::current();

			let task = task_once!(
				"flow-processing",
				High,
				move |_ctx: &TaskContext| -> reifydb_core::Result<()> {
					// Create child span linked to parent for distributed tracing
					let _guard = trace_span!(
						parent: parent_span,
						"flow::worker_task",
						flow_id = ?flow_id,
						unit_count = unit_count,
						change_count = change_count
					)
					.entered();

					process(&mut flow_txn, flow_units, &engine)?;
					let _ = result_tx.send(Ok((flow_id, flow_txn)));
					Ok(())
				}
			);

			self.scheduler.once(task)?;
		}

		// Drop our copy of sender so channel closes when all tasks complete
		drop(result_tx);
		drop(_submit_span);

		let _await_span = trace_span!("flow::await_results").entered();

		let mut completed: Vec<(FlowId, FlowTransaction)> = Vec::new();
		while let Ok(result) = result_rx.recv() {
			match result {
				Ok((flow_id, flow_txn)) => {
					completed.push((flow_id, flow_txn));
				}
				Err(e) => return e,
			}
		}

		drop(_await_span);

		// Sort by flow_id for deterministic commit order regardless of task completion order
		completed.sort_by_key(|(flow_id, _)| *flow_id);

		// Commit all FlowTransactions sequentially back to parent
		for (_seq, (_flow_id, mut flow)) in completed.into_iter().enumerate() {
			flow.commit(txn)?;
		}

		Ok(())
	}

	fn name(&self) -> &str {
		"parallel-worker-pool"
	}
}

/// Process all units for a single flow in parallel worker, returning completed FlowTransaction
fn process(flow_txn: &mut FlowTransaction, flow_units: Vec<UnitOfWork>, engine: &FlowEngine) -> crate::Result<()> {
	// Process all units for this flow sequentially
	for unit in flow_units {
		let _unit_span = trace_span!(
			"flow::process_unit",
			flow_id = ?unit.flow_id,
			version = unit.version.0,
			change_count = unit.source_changes.len()
		)
		.entered();

		// Update version if needed
		if flow_txn.version() != unit.version {
			flow_txn.update_version(unit.version);
		}

		// Process all source changes for this unit
		let flow_id = unit.flow_id;
		for change in unit.source_changes {
			engine.process(flow_txn, change, flow_id)?;
		}
	}

	Ok(())
}
