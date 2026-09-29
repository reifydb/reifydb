// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::RecordBatch;
use reifydb_value::value::duration::Duration;

use crate::interface::catalog::id::SubscriptionId;

pub trait SubscriptionInspector: Send + Sync {
	fn inspect(&self, id: SubscriptionId) -> Option<RecordBatch>;

	fn active_subscriptions(&self) -> Vec<SubscriptionId>;
}

pub type SubscriptionInspectorRef = Arc<dyn SubscriptionInspector>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HydrationConfig {
	pub enabled: bool,
	pub max_rows: Option<u64>,
}

impl Default for HydrationConfig {
	fn default() -> Self {
		Self {
			enabled: true,
			max_rows: None,
		}
	}
}

#[derive(Debug, Clone, Default)]
pub struct SubscribeOptions {
	pub hydration: HydrationConfig,
	pub throttle: Option<Duration>,
	pub linger: Option<Duration>,
}

pub enum SubscribeOutcome {
	Local {
		id: SubscriptionId,
	},
	Remote {
		address: String,
		body: String,
		token: Option<String>,
	},
}
