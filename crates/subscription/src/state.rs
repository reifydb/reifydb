// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Consumption state tracking for subscriptions.

use reifydb_core::{encoded::key::EncodedKey, interface::catalog::id::SubscriptionId};

/// Consumption state for a single subscription.
#[derive(Debug, Clone)]
pub struct ConsumptionState {
	/// The database subscription ID being consumed
	pub db_subscription_id: SubscriptionId,
	/// The last row key that was successfully consumed and deleted
	/// Used as a cursor for incremental polling
	pub last_consumed_key: Option<EncodedKey>,
}
