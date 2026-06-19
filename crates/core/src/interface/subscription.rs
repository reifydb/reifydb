// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use crate::interface::catalog::{id::SubscriptionId, shape::ShapeId};

#[derive(Debug, Clone)]
pub struct SubscriptionWatermarkRow {
	pub subscription_id: SubscriptionId,

	pub shape_id: ShapeId,

	pub lag: u64,
}

#[derive(Clone)]
pub struct SubscriptionWatermarkSampler {
	fetch: Arc<dyn Fn() -> Vec<SubscriptionWatermarkRow> + Send + Sync>,
}

impl SubscriptionWatermarkSampler {
	pub fn new<F>(fetch: F) -> Self
	where
		F: Fn() -> Vec<SubscriptionWatermarkRow> + Send + Sync + 'static,
	{
		Self {
			fetch: Arc::new(fetch),
		}
	}

	pub fn all(&self) -> Vec<SubscriptionWatermarkRow> {
		(self.fetch)()
	}
}
