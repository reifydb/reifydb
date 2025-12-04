// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::VecDeque;

use reifydb_engine::StandardCommandTransaction;

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

impl WorkerPool for SameThreadedWorker {
	fn process(
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
		let mut flow_txn = FlowTransaction::new(txn, first_unit.version);

		while let Some(unit_of_work) = queue.pop_front() {
			if flow_txn.version() != unit_of_work.version {
				flow_txn.update_version(unit_of_work.version)?;
			}

			let flow_id = unit_of_work.flow_id;
			for change in unit_of_work.source_changes {
				engine.process(&mut flow_txn, change, flow_id)?;
			}
		}

		flow_txn.commit(txn)?;

		Ok(())
	}

	fn name(&self) -> &str {
		"single-threaded"
	}
}
