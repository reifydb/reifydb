// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Provides flow lag data for the system.flow_lags virtual table (V2).

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_core::interface::{FlowLagRow, FlowLagsProvider};
use reifydb_engine::StandardEngine;

use crate::{registry::FlowConsumerRegistry, tracker::PrimitiveVersionTracker};

/// Provides flow lag data for virtual table queries (V2 implementation).
///
/// Computes per-source lag for each flow by comparing the flow's checkpoint
/// version to the latest change version of each source it subscribes to.
pub struct FlowLagsV2 {
	registry: Arc<FlowConsumerRegistry>,
	primitive_tracker: Arc<PrimitiveVersionTracker>,
	engine: StandardEngine,
}

impl FlowLagsV2 {
	/// Create a new provider.
	pub fn new(
		registry: Arc<FlowConsumerRegistry>,
		primitive_tracker: Arc<PrimitiveVersionTracker>,
		engine: StandardEngine,
	) -> Self {
		Self {
			registry,
			primitive_tracker,
			engine,
		}
	}
}

#[async_trait]
impl FlowLagsProvider for FlowLagsV2 {
	/// Get all flow lag rows.
	///
	/// Returns one row per (flow, source) pair, showing how far behind
	/// each flow is for each of its subscribed sources.
	async fn all_lags(&self) -> Vec<FlowLagRow> {
		let flow_info = self.registry.all_flow_info().await;
		let primitive_versions = self.primitive_tracker.all().await;

		let mut rows = Vec::new();

		for (flow_id, sources) in flow_info {
			// Get flow's current checkpoint version
			let flow_version = {
				let consumers = self.registry.consumers_read().await;
				if let Some(handle) = consumers.get(&flow_id) {
					match handle.flow_consumer.current_version(&self.engine).await {
						Ok(v) => v.0,
						Err(_) => 0, // Flow not yet processed anything
					}
				} else {
					continue; // Flow removed between queries
				}
			};

			for primitive_id in sources {
				let primitive_version = primitive_versions.get(&primitive_id).map(|v| v.0).unwrap_or(0);
				let lag = primitive_version.saturating_sub(flow_version);

				rows.push(FlowLagRow {
					flow_id,
					primitive_id,
					lag,
				});
			}
		}

		rows
	}
}
