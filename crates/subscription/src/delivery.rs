// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{interface::catalog::id::SubscriptionId, value::column::columns::Columns};
use reifydb_value::value::duration::Duration;
use tokio::sync::Notify;

#[derive(Debug)]
pub enum DeliveryResult {
	Delivered,

	Disconnected,
}

pub trait SubscriptionDelivery: Send + Sync {
	fn try_deliver(&self, subscription: &SubscriptionId, columns: Columns) -> DeliveryResult;

	fn active_subscriptions(&self) -> Vec<SubscriptionId>;

	fn active_subscriptions_into(&self, out: &mut Vec<SubscriptionId>) {
		out.extend(self.active_subscriptions());
	}

	fn flush(&self) -> Option<Duration> {
		None
	}

	fn register_waker(&self, _waker: Arc<Notify>) {}
}
