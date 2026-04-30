// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_cdc::consume::checkpoint::CdcCheckpoint;
use reifydb_core::{common::CommitVersion, interface::flow::FlowWatermarkRow};
use reifydb_engine::engine::StandardEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::identity::IdentityId;

use super::tracker::ShapeVersionTracker;
use crate::catalog::FlowCatalog;

/// Compute the current flow watermark rows.
///
/// Returns one row per (registered flow, source primitive) pair. Each row's
/// `lag` is the difference between the source's last seen version and the
/// flow's CDC checkpoint, so a value of 0 means the flow has caught up to
/// that source.
pub(crate) fn compute_flow_watermarks(
	primitive_tracker: &Arc<ShapeVersionTracker>,
	engine: &StandardEngine,
	catalog: &FlowCatalog,
) -> Vec<FlowWatermarkRow> {
	let primitive_versions = primitive_tracker.all();

	let mut txn = match engine.begin_query(IdentityId::system()) {
		Ok(txn) => txn,
		Err(_) => return Vec::new(),
	};

	let mut rows = Vec::new();

	let registered = catalog.get_flow_ids();

	for flow_id in &registered {
		let flow_version =
			CdcCheckpoint::fetch(&mut Transaction::Query(&mut txn), flow_id).unwrap_or(CommitVersion(0)).0;

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
