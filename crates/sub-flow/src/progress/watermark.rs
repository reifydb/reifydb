// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{common::CommitVersion, interface::flow::FlowWatermarkRow};

use crate::{
	catalog::FlowCatalog,
	progress::tracker::{FlowPositionTracker, ObjectVersionTracker},
};

pub(crate) fn compute_flow_watermarks(
	object_tracker: &ObjectVersionTracker,
	flow_tracker: &FlowPositionTracker,
	catalog: &FlowCatalog,
	consumable: impl Fn() -> CommitVersion,
) -> Vec<FlowWatermarkRow> {
	let object_versions = object_tracker.all();
	let flow_positions = flow_tracker.all();
	let consumable = consumable();

	let mut rows = Vec::new();

	let registered = catalog.get_flow_ids();

	for flow_id in &registered {
		let flow_version = flow_positions.get(flow_id).copied().unwrap_or(CommitVersion(0)).0;
		let outstanding = consumable.0.saturating_sub(flow_version);

		for (object_id, version) in &object_versions {
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
