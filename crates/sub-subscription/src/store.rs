// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::{HashMap, VecDeque},
	sync::{
		RwLock, RwLockReadGuard,
		atomic::{AtomicU64, Ordering},
	},
};

use dashmap::DashMap;
use reifydb_core::{interface::catalog::id::SubscriptionId, value::column::columns::Columns};

struct SubscriptionBuffer {
	queue: VecDeque<Columns>,
	capacity: usize,
	/// Column names for the subscription schema (includes implicit _op).
	column_names: Vec<String>,
}

/// Central store for all active subscription buffers.
///
/// Thread-safe: accessed by the subscription CDC consumer (via `commit_staged`
/// after each CDC pass), the poller (via `drain` during a `begin_poll` guard),
/// and DDL (register/unregister) concurrently.
///
/// Uses ring buffer semantics: when a buffer is full, the oldest entry is evicted
/// to make room for new data, preventing data loss due to backpressure.
pub struct SubscriptionStore {
	inner: DashMap<SubscriptionId, SubscriptionBuffer>,
	next_id: AtomicU64,
	default_capacity: usize,
	/// Coordinates the boundary between per-CDC-batch commits and poller drain cycles.
	/// Commits take write; poll cycles take read. Held briefly in both cases.
	coord: RwLock<()>,
}

impl SubscriptionStore {
	pub fn new(default_capacity: usize) -> Self {
		Self {
			inner: DashMap::new(),
			next_id: AtomicU64::new(1),
			default_capacity,
			coord: RwLock::new(()),
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

	/// Atomically apply a batch of staged pushes. From the poller's point of view,
	/// either all entries in `staged` become visible together, or none of them do.
	///
	/// Each subscription's staged diffs are appended in the given order, with
	/// ring-buffer eviction if the buffer is at capacity. Entries for
	/// subscriptions that no longer exist are silently dropped.
	pub fn commit_staged(&self, staged: HashMap<SubscriptionId, Vec<Columns>>) {
		if staged.is_empty() {
			return;
		}
		let _write = self.coord.write().unwrap();
		for (id, columns_vec) in staged {
			let Some(mut buf) = self.inner.get_mut(&id) else {
				continue;
			};
			for columns in columns_vec {
				if buf.queue.len() >= buf.capacity {
					buf.queue.pop_front();
				}
				buf.queue.push_back(columns);
			}
		}
	}

	/// Acquire a read guard for the duration of a poll cycle. While held,
	/// `commit_staged` is blocked, so the poller sees a consistent snapshot
	/// of what has been committed up to this point.
	pub fn begin_poll(&self) -> RwLockReadGuard<'_, ()> {
		self.coord.read().unwrap()
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

	fn stage(id: SubscriptionId, values: &[u8]) -> HashMap<SubscriptionId, Vec<Columns>> {
		let mut map = HashMap::new();
		map.insert(id, values.iter().copied().map(test_columns).collect());
		map
	}

	#[test]
	fn test_register_and_commit() {
		let store = SubscriptionStore::new(16);
		let id = store.next_id();
		store.register(id, vec!["test".to_string()]);

		store.commit_staged(stage(id, &[1]));

		let drained = store.drain(&id, 10);
		assert_eq!(drained.len(), 1);
	}

	#[test]
	fn test_commit_to_unregistered_is_dropped() {
		let store = SubscriptionStore::new(16);
		let id = SubscriptionId(999);

		store.commit_staged(stage(id, &[1]));

		let drained = store.drain(&id, 10);
		assert!(drained.is_empty());
	}

	#[test]
	fn test_ring_buffer_overwrites_oldest() {
		let store = SubscriptionStore::new(2);
		let id = store.next_id();
		store.register(id, vec!["test".to_string()]);

		// Three separate commits so each push evaluates buffer capacity
		// against the already-committed tail — mirrors how the subscription
		// CDC consumer drives the store one batch at a time.
		store.commit_staged(stage(id, &[1]));
		store.commit_staged(stage(id, &[2]));
		store.commit_staged(stage(id, &[3]));

		let drained = store.drain(&id, 10);
		assert_eq!(drained.len(), 2);
		// The oldest value(1) has been evicted; remaining are value(2) and value(3)
	}

	#[test]
	fn test_drain_partial_then_full() {
		let store = SubscriptionStore::new(16);
		let id = store.next_id();
		store.register(id, vec!["test".to_string()]);

		store.commit_staged(stage(id, &[1, 2, 3]));

		let drained = store.drain(&id, 2);
		assert_eq!(drained.len(), 2);

		let remaining = store.drain(&id, 10);
		assert_eq!(remaining.len(), 1);

		let empty = store.drain(&id, 10);
		assert!(empty.is_empty());
	}

	#[test]
	fn test_unregister_removes_from_active() {
		let store = SubscriptionStore::new(16);
		let id = store.next_id();
		store.register(id, vec!["test".to_string()]);

		assert!(store.active_subscriptions().contains(&id));
		assert!(store.unregister(&id));
		assert!(!store.active_subscriptions().contains(&id));
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
