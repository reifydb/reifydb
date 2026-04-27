// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Flow watermark interface for virtual table support.

use std::sync::Arc;

use crate::interface::catalog::{flow::FlowId, shape::ShapeId};

/// A row in the system.flow_watermarks virtual table.
#[derive(Debug, Clone)]
pub struct FlowWatermarkRow {
	/// The flow ID.
	pub flow_id: FlowId,
	/// The shape this flow subscribes to.
	pub shape_id: ShapeId,
	/// The lag: how many versions behind the flow is for this source.
	pub lag: u64,
}

/// Concrete IoC service that yields flow watermark rows.
///
/// The flow subsystem constructs one of these during startup with a closure
/// that captures its internal state (tracker, engine, flow catalog). The
/// `system.flow_watermarks` virtual table and `db.watermarks().flow()`
/// resolve it from IoC by concrete type.
///
/// Lives in `core` so downstream crates (catalog, pkg/reifydb) can name it
/// without depending on `sub-flow` directly.
#[derive(Clone)]
pub struct FlowWatermarkSampler {
	fetch: Arc<dyn Fn() -> Vec<FlowWatermarkRow> + Send + Sync>,
}

impl FlowWatermarkSampler {
	pub fn new<F>(fetch: F) -> Self
	where
		F: Fn() -> Vec<FlowWatermarkRow> + Send + Sync + 'static,
	{
		Self {
			fetch: Arc::new(fetch),
		}
	}

	pub fn all(&self) -> Vec<FlowWatermarkRow> {
		(self.fetch)()
	}
}
