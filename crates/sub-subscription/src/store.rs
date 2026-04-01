// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::VecDeque,
	sync::atomic::{AtomicU64, Ordering},
};

use dashmap::DashMap;
use reifydb_core::{interface::catalog::id::SubscriptionId, value::column::columns::Columns};

/// Result of pushing data into a subscription buffer.
#[derive(Debug, PartialEq)]
pub enum PushResult {
	/// Data was accepted into the buffer.
	Accepted,
	/// Subscription does not exist or has been unregistered.
	NotFound,
}

struct SubscriptionBuffer {
	queue: VecDeque<Columns>,
	capacity: usize,
	/// Column names for the subscription schema (includes implicit _op).
	column_names: Vec<String>,
}

/// Central store for all active subscription buffers.
///
/// Thread-safe: accessed by sink operators (push), pollers/consumers (pop/drain),
/// and DDL (register/unregister) concurrently.
///
/// Uses ring buffer semantics: when a buffer is full, the oldest entry is evicted
/// to make room for new data, preventing data loss due to backpressure.
pub struct SubscriptionStore {
	inner: DashMap<SubscriptionId, SubscriptionBuffer>,
	next_id: AtomicU64,
	default_capacity: usize,
}

impl SubscriptionStore {
	pub fn new(default_capacity: usize) -> Self {
		Self {
			inner: DashMap::new(),
			next_id: AtomicU64::new(1),
			default_capacity,
		}
	}

	/// Generate a new unique SubscriptionId.
	pub fn next_id(&self) -> SubscriptionId {
		SubscriptionId(self.next_id.fetch_add(1, Ordering::Relaxed))
	}

	/// Register a subscription with a bounded buffer and column schema.
	pub fn register(&self, id: SubscriptionId, column_names: Vec<String>) {
		self.inner.insert(
			id,
			SubscriptionBuffer {
				queue: VecDeque::with_capacity(self.default_capacity),
				capacity: self.default_capacity,
				column_names,
			},
		);
	}

	/// Get the column names for a subscription's schema.
	pub fn column_names(&self, id: &SubscriptionId) -> Option<Vec<String>> {
		self.inner.get(id).map(|buf| buf.column_names.clone())
	}

	/// Unregister a subscription, dropping its buffer.
	/// Returns true if it existed.
	pub fn unregister(&self, id: &SubscriptionId) -> bool {
		self.inner.remove(id).is_some()
	}

	/// Push a Columns batch into a subscription's buffer.
	/// Called by the sink operator after flow processing.
	///
	/// Uses ring buffer semantics: if the buffer is at capacity, the oldest
	/// entry is evicted before inserting the new data.
	pub fn push(&self, id: &SubscriptionId, columns: Columns) -> PushResult {
		match self.inner.get_mut(id) {
			Some(mut buf) => {
				if buf.queue.len() >= buf.capacity {
					buf.queue.pop_front(); // evict oldest entry
				}
				buf.queue.push_back(columns);
				PushResult::Accepted
			}
			None => PushResult::NotFound,
		}
	}

	/// Pop a single batch from a subscription's buffer.
	/// Returns None if the buffer is empty or subscription doesn't exist.
	pub fn pop(&self, id: &SubscriptionId) -> Option<Columns> {
		self.inner.get_mut(id).and_then(|mut buf| buf.queue.pop_front())
	}

	/// Drain up to `max_batches` from a subscription's buffer.
	/// Non-blocking: returns immediately with whatever is available.
	pub fn drain(&self, id: &SubscriptionId, max_batches: usize) -> Vec<Columns> {
		match self.inner.get_mut(id) {
			Some(mut buf) => {
				let count = max_batches.min(buf.queue.len());
				buf.queue.drain(..count).collect()
			}
			None => Vec::new(),
		}
	}

	/// Get the list of active subscription IDs.
	pub fn active_subscriptions(&self) -> Vec<SubscriptionId> {
		self.inner.iter().map(|entry| *entry.key()).collect()
	}

	/// Check if a subscription exists.
	pub fn is_active(&self, id: &SubscriptionId) -> bool {
		self.inner.contains_key(id)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::value::column::{Column, data::ColumnData};
	use reifydb_type::fragment::Fragment;

	use super::*;

	fn test_columns(value: u8) -> Columns {
		Columns::new(vec![Column {
			name: Fragment::internal("test"),
			data: ColumnData::uint1(vec![value]),
		}])
	}

	#[test]
	fn test_register_and_push() {
		let store = SubscriptionStore::new(16);
		let id = store.next_id();
		store.register(id, vec!["test".to_string()]);

		let result = store.push(&id, test_columns(1));
		assert_eq!(result, PushResult::Accepted);

		let popped = store.pop(&id);
		assert!(popped.is_some());
	}

	#[test]
	fn test_push_to_unregistered() {
		let store = SubscriptionStore::new(16);
		let id = SubscriptionId(999);

		let result = store.push(&id, test_columns(1));
		assert_eq!(result, PushResult::NotFound);
	}

	#[test]
	fn test_ring_buffer_overwrites_oldest() {
		let store = SubscriptionStore::new(2);
		let id = store.next_id();
		store.register(id, vec!["test".to_string()]);

		// Fill to capacity
		assert_eq!(store.push(&id, test_columns(1)), PushResult::Accepted);
		assert_eq!(store.push(&id, test_columns(2)), PushResult::Accepted);

		// Third push should evict value(1) and accept value(3)
		assert_eq!(store.push(&id, test_columns(3)), PushResult::Accepted);

		// Should have 2 items (oldest evicted)
		let drained = store.drain(&id, 10);
		assert_eq!(drained.len(), 2);
		// The oldest value(1) has been evicted; remaining are value(2) and value(3)
	}

	#[test]
	fn test_drain() {
		let store = SubscriptionStore::new(16);
		let id = store.next_id();
		store.register(id, vec!["test".to_string()]);

		store.push(&id, test_columns(1));
		store.push(&id, test_columns(2));
		store.push(&id, test_columns(3));

		let drained = store.drain(&id, 2);
		assert_eq!(drained.len(), 2);

		// One remaining
		let remaining = store.drain(&id, 10);
		assert_eq!(remaining.len(), 1);

		// Empty
		let empty = store.drain(&id, 10);
		assert!(empty.is_empty());
	}

	#[test]
	fn test_unregister() {
		let store = SubscriptionStore::new(16);
		let id = store.next_id();
		store.register(id, vec!["test".to_string()]);

		assert!(store.is_active(&id));
		assert!(store.unregister(&id));
		assert!(!store.is_active(&id));
		assert!(!store.unregister(&id));
	}

	#[test]
	fn test_active_subscriptions() {
		let store = SubscriptionStore::new(16);
		let id1 = store.next_id();
		let id2 = store.next_id();
		store.register(id1, vec![]);
		store.register(id2, vec![]);

		let active = store.active_subscriptions();
		assert_eq!(active.len(), 2);
		assert!(active.contains(&id1));
		assert!(active.contains(&id2));
	}

	#[test]
	fn test_unique_ids() {
		let store = SubscriptionStore::new(16);
		let id1 = store.next_id();
		let id2 = store.next_id();
		let id3 = store.next_id();
		assert_ne!(id1, id2);
		assert_ne!(id2, id3);
	}
}
