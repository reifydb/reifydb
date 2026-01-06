// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Simple per-flow consumer struct for single-threaded flow processing.

use std::{
	collections::HashSet,
	sync::atomic::{AtomicU64, Ordering},
};

use reifydb_core::{
	CommitVersion, Result,
	interface::{FlowId, PrimitiveId},
};
use reifydb_rql::flow::{Flow, FlowNodeType};
use reifydb_sdk::{FlowChange, FlowChangeOrigin};

use crate::{FlowEngine, FlowTransaction};

/// Per-flow consumer that processes CDC changes.
///
/// This is a simple struct (not a task) that:
/// - Holds the flow definition and source primitives
/// - Filters changes relevant to this flow
/// - Delegates processing to FlowEngine
pub struct FlowConsumer {
	/// Flow identifier
	flow_id: FlowId,

	/// Flow definition
	#[allow(dead_code)]
	flow: Flow,

	/// Source primitives this flow subscribes to
	sources: HashSet<PrimitiveId>,

	/// Current processed version (for FlowLags queries)
	current_version: AtomicU64,
}

impl FlowConsumer {
	/// Create a new flow consumer.
	pub fn new(flow_id: FlowId, flow: Flow) -> Self {
		let sources = extract_sources(&flow);

		Self {
			flow_id,
			flow,
			sources,
			current_version: AtomicU64::new(0),
		}
	}

	/// Process a version's changes for this flow.
	///
	/// Filters the pre-decoded changes to only those relevant to this flow's
	/// sources, then delegates to FlowEngine.process().
	pub fn process(
		&self,
		txn: &mut FlowTransaction,
		flow_engine: &FlowEngine,
		changes: &[FlowChange],
	) -> Result<()> {
		// Filter changes to only those from our sources
		for change in changes {
			match &change.origin {
				FlowChangeOrigin::External(source) if self.sources.contains(source) => {
					flow_engine.process(txn, change.clone(), self.flow_id)?;
				}
				_ => {}
			}
		}

		Ok(())
	}

	/// Update the current version after successful processing.
	pub fn set_version(&self, version: CommitVersion) {
		self.current_version.store(version.0, Ordering::Release);
	}

	/// Get the current processed version.
	#[allow(dead_code)]
	pub fn current_version(&self) -> CommitVersion {
		CommitVersion(self.current_version.load(Ordering::Acquire))
	}

	/// Get the flow ID.
	#[allow(dead_code)]
	pub fn flow_id(&self) -> FlowId {
		self.flow_id
	}

	/// Get the source primitives for this flow.
	#[allow(dead_code)]
	pub fn sources(&self) -> &HashSet<PrimitiveId> {
		&self.sources
	}

	/// Check if this flow has any of the given primitives as sources.
	pub fn has_sources(&self, primitives: &HashSet<PrimitiveId>) -> bool {
		!self.sources.is_disjoint(primitives)
	}
}

/// Extract source primitive IDs from a flow definition.
fn extract_sources(flow: &Flow) -> HashSet<PrimitiveId> {
	flow.graph
		.nodes()
		.filter_map(|(_, node)| match &node.ty {
			FlowNodeType::SourceTable {
				table,
			} => Some(PrimitiveId::Table(*table)),
			FlowNodeType::SourceView {
				view,
			} => Some(PrimitiveId::View(*view)),
			FlowNodeType::SourceFlow {
				flow,
			} => Some(PrimitiveId::Flow(*flow)),
			_ => None,
		})
		.collect()
}
