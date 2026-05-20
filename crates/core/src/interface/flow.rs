// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use crate::interface::catalog::{flow::FlowId, shape::ShapeId};

#[derive(Debug, Clone)]
pub struct FlowWatermarkRow {
	pub flow_id: FlowId,

	pub shape_id: ShapeId,

	pub lag: u64,
}

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
