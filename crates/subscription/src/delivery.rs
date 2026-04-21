// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::id::SubscriptionId, value::column::columns::Columns};

/// Result of attempting to deliver a subscription frame.
#[derive(Debug)]
pub enum DeliveryResult {
	/// Frame was successfully delivered.
	Delivered,
	/// Consumer has disconnected. Subscription should be cleaned up.
	Disconnected,
}

/// Trait for delivering subscription data to consumers.
///
/// Implementations handle the protocol-specific details of sending
/// subscription frames to clients.
pub trait SubscriptionDelivery: Send + Sync {
	/// Try to deliver columns to the subscription's consumer.
	fn try_deliver(&self, subscription: &SubscriptionId, columns: Columns) -> DeliveryResult;

	/// Get the list of currently active subscription IDs.
	fn active_subscriptions(&self) -> Vec<SubscriptionId>;

	/// Flush any delivery aggregation state. Called once per poller tick
	/// after all `try_deliver` calls in that tick. Default impl is a no-op;
	/// batch-aware implementations (see batch subscriptions) override this
	/// to emit coalesced per-tick envelopes.
	fn flush(&self) {}
}
