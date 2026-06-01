// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{common::CommitVersion, interface::flow::FlowWatermarkRow};

use super::tracker::{FlowPositionTracker, ShapeVersionTracker};
use crate::catalog::FlowCatalog;

pub(crate) fn compute_flow_watermarks(
	primitive_tracker: &Arc<ShapeVersionTracker>,
	flow_tracker: &Arc<FlowPositionTracker>,
	catalog: &FlowCatalog,
) -> Vec<FlowWatermarkRow> {
	let primitive_versions = primitive_tracker.all();
	let flow_positions = flow_tracker.all();

	let mut rows = Vec::new();

	let registered = catalog.get_flow_ids();

	for flow_id in &registered {
		let flow_version = flow_positions.get(flow_id).copied().unwrap_or(CommitVersion(0)).0;

		for (shape_id, version) in &primitive_versions {
			let lag = version.0.saturating_sub(flow_version);
			rows.push(FlowWatermarkRow {
				flow_id: *flow_id,
				shape_id: *shape_id,
				lag,
			});
		}
	}

	rows
}
