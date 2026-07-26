// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{common::CommitVersion, interface::flow::FlowWatermarkRow};

use super::tracker::{FlowPositionTracker, ObjectVersionTracker};
use crate::catalog::FlowCatalog;

pub(crate) fn compute_flow_watermarks(
	primitive_tracker: &ObjectVersionTracker,
	flow_tracker: &FlowPositionTracker,
	catalog: &FlowCatalog,
	consumable: impl Fn() -> CommitVersion,
) -> Vec<FlowWatermarkRow> {
	let primitive_versions = primitive_tracker.all();
	let flow_positions = flow_tracker.all();
	let consumable = consumable();

	let mut rows = Vec::new();

	let registered = catalog.get_flow_ids();

	for flow_id in &registered {
		let flow_version = flow_positions.get(flow_id).copied().unwrap_or(CommitVersion(0)).0;
		let outstanding = consumable.0.saturating_sub(flow_version);

		for (object_id, version) in &primitive_versions {
			let lag = version.0.saturating_sub(flow_version);
			rows.push(FlowWatermarkRow {
				flow_id: *flow_id,
				object_id: *object_id,
				lag,
				outstanding,
			});
		}
	}

	rows
}
