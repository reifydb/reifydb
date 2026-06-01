// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::interface::catalog::id::SubscriptionId;
use reifydb_sub_subscription::store::SubscriptionStore;
use reifydb_value::value::frame::frame::Frame;

/// Handle to a created subscription. Owns a reference to the delivery store, so it can drain
/// delivered batches without going back through the `Database`. A handle obtained before a
/// `Database::stop()` is stale afterwards; re-attach to the same subscription id with
/// `Database::subscription`.
pub struct Subscription {
	id: SubscriptionId,
	store: Arc<SubscriptionStore>,
	column_names: Vec<String>,
}

impl Subscription {
	pub(crate) fn new(id: SubscriptionId, store: Arc<SubscriptionStore>, column_names: Vec<String>) -> Self {
		Self {
			id,
			store,
			column_names,
		}
	}

	pub fn id(&self) -> SubscriptionId {
		self.id
	}

	pub fn column_names(&self) -> &[String] {
		&self.column_names
	}

	/// Drain up to `max` delivered batches as `Frame`s. Each row carries an `_op` column
	/// (Insert=1, Update=2, Remove=3). Returns the batches in delivery order and removes them
	/// from the buffer.
	pub fn drain(&self, max: usize) -> Vec<Frame> {
		self.store.drain(&self.id, max).into_iter().map(Frame::from).collect()
	}
}
