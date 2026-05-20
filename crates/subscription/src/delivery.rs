// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

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
