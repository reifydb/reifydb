// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::interface::catalog::id::SubscriptionId;
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_sub_subscription::store::SubscriptionStore;
use reifydb_value::value::frame::frame::Frame;

/// Handle to a created subscription. Owns a reference to the delivery store, so it can drain
/// delivered batches without going back through the `Database`. A handle obtained before a
/// `Database::stop()` is stale afterwards; re-attach to the same subscription id with
/// `Database::subscription`.
///
/// When the subscription is created with hydration enabled, the initial-snapshot batches are
/// captured into `prelude` and drained ahead of any forward CDC the store buffers, so a subscriber
/// observes the current state before subsequent changes.
pub struct Subscription {
	id: SubscriptionId,
	store: Arc<SubscriptionStore>,
	column_names: Vec<String>,
	prelude: Mutex<Vec<Frame>>,
}

impl Subscription {
	pub(crate) fn new(
		id: SubscriptionId,
		store: Arc<SubscriptionStore>,
		column_names: Vec<String>,
		prelude: Vec<Frame>,
	) -> Self {
		Self {
			id,
			store,
			column_names,
			prelude: Mutex::new(prelude),
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
	/// from the buffer. Hydration-snapshot batches (if any) are returned before forward-CDC
	/// batches.
	pub fn drain(&self, max: usize) -> Vec<Frame> {
		let mut out: Vec<Frame> = Vec::new();
		{
			let mut prelude = self.prelude.lock();
			if !prelude.is_empty() {
				let take = max.min(prelude.len());
				out.extend(prelude.drain(..take));
			}
		}
		if out.len() < max {
			let remaining = max - out.len();
			out.extend(self.store.drain(&self.id, remaining).into_iter().map(Frame::from));
		}
		out
	}
}
