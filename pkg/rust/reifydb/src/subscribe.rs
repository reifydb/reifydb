// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::interface::catalog::id::SubscriptionId;
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_sub_subscription::store::SubscriptionStore;
use reifydb_value::value::frame::frame::Frame;

/// A handle obtained before `Database::stop()` is stale afterwards; re-attach to the same id with
/// `Database::subscription`. Hydration-snapshot batches drain ahead of any forward CDC the store
/// buffers, so a subscriber observes the current state before subsequent changes.
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

	/// Each row carries an `_op` column (Insert=1, Update=2, Remove=3). Batches come back in
	/// delivery order and are removed from the buffer, hydration-snapshot ones ahead of
	/// forward-CDC ones.
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
