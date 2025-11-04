// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub mod parallel;
pub mod processor;
pub mod same;

pub use parallel::ParallelWorkerPool;
pub use processor::WorkerPool;
use reifydb_core::{CommitVersion, interface::FlowId};
pub use same::SameThreadedWorker;

use crate::flow::FlowChange;

/// A unit of work representing a flow and all its source changes
#[derive(Debug, Clone)]
pub struct UnitOfWork {
	/// The flow to process
	pub flow_id: FlowId,

	/// The commit version for this unit of work
	pub version: CommitVersion,

	/// All source changes this flow needs to process
	/// Multiple entries if flow subscribes to multiple sources (e.g., joins)
	pub source_changes: Vec<FlowChange>,
}

impl UnitOfWork {
	/// Create a new unit of work
	pub fn new(flow_id: FlowId, version: CommitVersion, source_changes: Vec<FlowChange>) -> Self {
		Self {
			flow_id,
			version,
			source_changes,
		}
	}
}

/// A collection of units of work grouped by flow
///
/// Each inner vector represents one flow's units, ordered by version.
/// The flow's units must be processed sequentially to maintain version ordering.
/// Different flows (outer vector) can be processed in parallel.
#[derive(Debug, Clone)]
pub struct UnitsOfWork(Vec<Vec<UnitOfWork>>);

impl UnitsOfWork {
	/// Create a new UnitsOfWork from a vector of flow units
	pub fn new(flow_units: Vec<Vec<UnitOfWork>>) -> Self {
		Self(flow_units)
	}

	/// Create an empty UnitsOfWork
	pub fn empty() -> Self {
		Self(Vec::new())
	}

	/// Check if there are no units of work
	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	/// Get the number of flows
	pub fn flow_count(&self) -> usize {
		self.0.len()
	}

	/// Get the total number of units across all flows
	pub fn total_units(&self) -> usize {
		self.0.iter().map(|units| units.len()).sum()
	}

	/// Get a reference to the inner vector
	pub fn as_inner(&self) -> &Vec<Vec<UnitOfWork>> {
		&self.0
	}

	/// Consume self and return the inner vector
	pub fn into_inner(self) -> Vec<Vec<UnitOfWork>> {
		self.0
	}
}

impl From<Vec<Vec<UnitOfWork>>> for UnitsOfWork {
	fn from(flow_units: Vec<Vec<UnitOfWork>>) -> Self {
		Self(flow_units)
	}
}

impl IntoIterator for UnitsOfWork {
	type Item = Vec<UnitOfWork>;
	type IntoIter = std::vec::IntoIter<Vec<UnitOfWork>>;

	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}
