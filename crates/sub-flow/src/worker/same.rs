// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::VecDeque;

use async_trait::async_trait;
use reifydb_engine::StandardCommandTransaction;
use tracing::trace_span;

use super::{UnitOfWork, UnitsOfWork, WorkerPool};
use crate::{engine::FlowEngine, transaction::FlowTransaction};

/// Same threaded worker that processes units of work sequentially from a deque
///
/// This is the initial implementation that can be easily replaced
/// with parallel implementations later.
pub struct SameThreadedWorker;

impl SameThreadedWorker {
	pub fn new() -> Self {
		Self {}
	}
}

#[async_trait]
impl WorkerPool for SameThreadedWorker {
	async fn process(
		&self,
		txn: &mut StandardCommandTransaction,
		units: UnitsOfWork,
		engine: &FlowEngine,
	) -> crate::Result<()> {
		// Flatten all flow units into a single queue for sequential processing
		let mut queue: VecDeque<UnitOfWork> = units.into_iter().flat_map(|units| units.into_iter()).collect();

		if queue.is_empty() {
			return Ok(());
		}

		let first_unit = queue.front().unwrap();
		let mut flow_txn = FlowTransaction::new(txn, first_unit.version).await;

		{
			let _loop_span = trace_span!("flow::process_all_units", unit_count = queue.len()).entered();
		}

		while let Some(unit_of_work) = queue.pop_front() {
			{
				let _unit_span = trace_span!(
					"flow::process_unit",
					flow_id = ?unit_of_work.flow_id,
					version = unit_of_work.version.0,
					change_count = unit_of_work.source_changes.len()
				)
				.entered();
			}

			if flow_txn.version() != unit_of_work.version {
				flow_txn.update_version(unit_of_work.version);
			}

			let flow_id = unit_of_work.flow_id;
			for change in unit_of_work.source_changes {
				engine.process(&mut flow_txn, change, flow_id).await?;
			}
		}
		flow_txn.commit(txn).await?;

		Ok(())
	}

	fn name(&self) -> &str {
		"single-threaded"
	}
}
