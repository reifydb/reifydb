// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Provides flow lag data for the system.flow_lags virtual table.

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_core::interface::{FlowLagRow, FlowLagsProvider};

use crate::{registry::FlowRegistry, tracker::PrimitiveVersionTracker};

/// Provides flow lag data for virtual table queries.
///
/// Computes per-source lag for each flow by comparing the flow's processed
/// version to the latest change version of each source it subscribes to.
///
/// Implements [`FlowLagsProvider`] trait for use by the engine's virtual table.
pub struct FlowLags {
	registry: Arc<FlowRegistry>,
	primitive_tracker: Arc<PrimitiveVersionTracker>,
}

impl FlowLags {
	/// Create a new provider.
	pub fn new(registry: Arc<FlowRegistry>, primitive_tracker: Arc<PrimitiveVersionTracker>) -> Self {
		Self {
			registry,
			primitive_tracker,
		}
	}
}

#[async_trait]
impl FlowLagsProvider for FlowLags {
	/// Get all flow lag rows.
	///
	/// Returns one row per (flow, source) pair, showing how far behind
	/// each flow is for each of its subscribed sources.
	async fn all_lags(&self) -> Vec<FlowLagRow> {
		let flow_data = self.registry.all_flow_data().await;
		let primitive_versions = self.primitive_tracker.all().await;

		let mut rows = Vec::new();

		for (flow_id, flow_version, sources) in flow_data {
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
