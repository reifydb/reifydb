// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use crate::interface::catalog::{id::SubscriptionId, object::ObjectId};

#[derive(Debug, Clone)]
pub struct SubscriptionPositionRow {
	pub subscription_id: SubscriptionId,

	pub object_id: ObjectId,

	pub lag: u64,
}

#[derive(Clone)]
pub struct SubscriptionPositionSampler {
	fetch: Arc<dyn Fn() -> Vec<SubscriptionPositionRow> + Send + Sync>,
}

impl SubscriptionPositionSampler {
	pub fn new<F>(fetch: F) -> Self
	where
		F: Fn() -> Vec<SubscriptionPositionRow> + Send + Sync + 'static,
	{
		Self {
			fetch: Arc::new(fetch),
		}
	}

	pub fn all(&self) -> Vec<SubscriptionPositionRow> {
		(self.fetch)()
	}
}
