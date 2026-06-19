// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use crate::{interface::catalog::id::SubscriptionId, value::column::columns::Columns};

pub const IMPLICIT_COLUMN_OP: &str = "_op";

pub trait SubscriptionInspector: Send + Sync {
	fn inspect(&self, id: SubscriptionId) -> Option<Columns>;

	fn active_subscriptions(&self) -> Vec<SubscriptionId>;

	fn column_count(&self, id: &SubscriptionId) -> Option<usize>;
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
