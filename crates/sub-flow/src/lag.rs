// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Provides flow lag data for the system.flow_lags virtual table.

use std::sync::Arc;

use reifydb_cdc::CdcCheckpoint;
use reifydb_core::{
	CommitVersion,
	interface::{FlowLagRow, FlowLagsProvider},
};
use reifydb_engine::StandardEngine;

use crate::{FlowEngine, tracker::PrimitiveVersionTracker};

/// Provides flow lag data for virtual table queries.
///
/// Each flow's progress is tracked individually via per-flow CDC checkpoints.
/// This enables accurate per-flow lag reporting and supports exactly-once
/// processing semantics during backfill restarts.
pub struct FlowLags {
	primitive_tracker: Arc<PrimitiveVersionTracker>,
	flow_engine: Arc<FlowEngine>,
	engine: StandardEngine,
}

impl FlowLags {
	/// Create a new flow lags provider.
	pub fn new_simple(
		primitive_tracker: Arc<PrimitiveVersionTracker>,
		flow_engine: Arc<FlowEngine>,
		engine: StandardEngine,
	) -> Self {
		Self {
			primitive_tracker,
			flow_engine,
			engine,
		}
	}
}

impl FlowLagsProvider for FlowLags {
	/// Get all flow lag rows.
	///
	/// Returns one row per (flow, primitive) pair, showing how far behind
	/// each flow is for each source primitive.
	fn all_lags(&self) -> Vec<FlowLagRow> {
		let primitive_versions = self.primitive_tracker.all();

		let mut txn = match self.engine.begin_query() {
			Ok(txn) => txn,
			Err(_) => return Vec::new(),
		};

		let mut rows = Vec::new();
		for flow_id in self.flow_engine.flow_ids() {
			let flow_version = CdcCheckpoint::fetch(&mut txn, &flow_id).unwrap_or(CommitVersion(0)).0;

			// Calculate lag for each primitive
			for (primitive_id, version) in &primitive_versions {
				let lag = version.0.saturating_sub(flow_version);
				rows.push(FlowLagRow {
					flow_id,
					primitive_id: *primitive_id,
					lag,
				});
			}
		}

		rows
	}
}
