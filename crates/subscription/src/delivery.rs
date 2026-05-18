// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::id::SubscriptionId, value::column::columns::Columns};

#[derive(Debug)]
pub enum DeliveryResult {
	Delivered,

	Disconnected,
}

pub trait SubscriptionDelivery: Send + Sync {
	fn try_deliver(&self, subscription: &SubscriptionId, columns: Columns) -> DeliveryResult;

	fn active_subscriptions(&self) -> Vec<SubscriptionId>;

	fn flush(&self) {}
}
