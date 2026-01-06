// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Provides flow lag data for the system.flow_lags virtual table.

use std::sync::Arc;

use reifydb_core::interface::{FlowLagRow, FlowLagsProvider};
use reifydb_engine::StandardEngine;

use crate::tracker::PrimitiveVersionTracker;

/// Provides flow lag data for virtual table queries (simplified implementation).
///
/// In the single-threaded model, all flows process together at the same version,
/// so lag is computed based on primitive tracker vs the global flow loop checkpoint.
pub struct FlowLags {
	primitive_tracker: Arc<PrimitiveVersionTracker>,
	engine: StandardEngine,
}

impl FlowLags {
	/// Create a new simplified provider (single-threaded model).
	pub fn new_simple(primitive_tracker: Arc<PrimitiveVersionTracker>, engine: StandardEngine) -> Self {
		Self {
			primitive_tracker,
			engine,
		}
	}
}

impl FlowLagsProvider for FlowLags {
	/// Get all flow lag rows.
	///
	/// In the single-threaded model, returns lag based on the global flow loop checkpoint.
	/// All flows are at the same version, so we return one row per primitive.
	fn all_lags(&self) -> Vec<FlowLagRow> {
		use reifydb_cdc::CdcCheckpoint;
		use reifydb_core::interface::CdcConsumerId;

		let primitive_versions = self.primitive_tracker.all();

		// Get the flow loop's current checkpoint
		let flow_loop_version = {
			let consumer_id = CdcConsumerId::new("flow-loop");
			let mut txn = match self.engine.begin_query() {
				Ok(txn) => txn,
				Err(_) => return Vec::new(),
			};
			CdcCheckpoint::fetch(&mut txn, &consumer_id).unwrap_or(reifydb_core::CommitVersion(0)).0
		};

		// Return one row per primitive showing lag
		primitive_versions
			.into_iter()
			.map(|(primitive_id, version)| {
				let lag = version.0.saturating_sub(flow_loop_version);
				FlowLagRow {
					flow_id: reifydb_core::interface::FlowId(0), /* No specific flow in
					                                              * single-threaded model */
					primitive_id,
					lag,
				}
			})
			.collect()
	}
}
