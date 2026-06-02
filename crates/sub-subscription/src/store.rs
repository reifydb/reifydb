// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, VecDeque},
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use dashmap::{DashMap, DashSet};
use reifydb_core::{interface::catalog::id::SubscriptionId, value::column::columns::Columns};
use reifydb_runtime::{
	reifydb_assertions,
	sync::{
		mutex::Mutex,
		rwlock::{RwLock, RwLockReadGuard},
	},
};
use tokio::sync::Notify;

struct SubscriptionBuffer {
	queue: VecDeque<Columns>,
	capacity: usize,

	column_names: Vec<String>,
}

pub struct SubscriptionStore {
	inner: DashMap<SubscriptionId, SubscriptionBuffer>,
	next_id: AtomicU64,
	default_capacity: usize,

	coord: RwLock<()>,
	wakers: Mutex<Vec<Arc<Notify>>>,

	hydrating: DashSet<SubscriptionId>,
}

impl SubscriptionStore {
	pub fn new(default_capacity: usize) -> Self {
		Self {
			inner: DashMap::new(),
			next_id: AtomicU64::new(1),
			default_capacity,
			coord: RwLock::new(()),
			wakers: Mutex::new(Vec::new()),
			hydrating: DashSet::new(),
		}
	}

	pub fn begin_hydration(&self, id: SubscriptionId) {
		self.hydrating.insert(id);
	}

	pub fn end_hydration(&self, id: &SubscriptionId) {
		self.hydrating.remove(id);
	}

	pub fn is_hydrating(&self, id: &SubscriptionId) -> bool {
		self.hydrating.contains(id)
	}

	pub fn register_waker(&self, waker: Arc<Notify>) {
		self.wakers.lock().push(waker);
	}

	pub fn next_id(&self) -> SubscriptionId {
		let raw = self.next_id.fetch_add(1, Ordering::Relaxed);
		reifydb_assertions! {
			assert!(
				raw != 0,
				"the subscription id counter wrapped past u64::MAX and issued 0, so a new subscriber collides with the reserved initial id and would receive another subscriber's delivery stream (issued={})",
				raw
			);
		}
		SubscriptionId(raw)
	}

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

	pub fn column_names(&self, id: &SubscriptionId) -> Option<Vec<String>> {
		self.inner.get(id).map(|buf| buf.column_names.clone())
	}

	pub fn unregister(&self, id: &SubscriptionId) -> bool {
		self.inner.remove(id).is_some()
	}

	pub fn drain(&self, id: &SubscriptionId, max_batches: usize) -> Vec<Columns> {
		match self.inner.get_mut(id) {
			Some(mut buf) => {
				let count = max_batches.min(buf.queue.len());
				buf.queue.drain(..count).collect()
			}
			None => Vec::new(),
		}
	}

	pub fn drain_into(&self, id: &SubscriptionId, max_batches: usize, out: &mut Vec<Columns>) {
		if let Some(mut buf) = self.inner.get_mut(id) {
			let count = max_batches.min(buf.queue.len());
			out.extend(buf.queue.drain(..count));
		}
	}

	pub fn active_subscriptions(&self) -> Vec<SubscriptionId> {
		self.inner.iter().map(|entry| *entry.key()).collect()
	}

	pub fn commit_staged(&self, staged: HashMap<SubscriptionId, Vec<Columns>>) {
		if staged.is_empty() {
			return;
		}
		self.append_staged_under_coord(staged);
		self.notify_wakers();
	}

	#[inline]
	fn append_staged_under_coord(&self, staged: HashMap<SubscriptionId, Vec<Columns>>) {
		let _write = self.coord.write();
		for (id, columns_vec) in staged {
			let Some(mut buf) = self.inner.get_mut(&id) else {
				continue;
			};
			for columns in columns_vec {
				reifydb_assertions! {
					let cap = buf.capacity;
					assert!(
						cap != 0,
						"subscription {:?} ring buffer has zero capacity, so the eviction branch can never run and push_back grows the queue without bound, leaking memory per committed batch",
						id
					);
				}
				if buf.queue.len() >= buf.capacity {
					buf.queue.pop_front();
				}
				buf.queue.push_back(columns);
			}
		}
	}

	#[inline]
	fn notify_wakers(&self) {
		for waker in self.wakers.lock().iter() {
			waker.notify_one();
		}
	}

	pub fn begin_poll(&self) -> RwLockReadGuard<'_, ()> {
		self.coord.read()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer};
	use reifydb_value::fragment::Fragment;

	use super::*;

	fn test_columns(value: u8) -> Columns {
		Columns::new(vec![ColumnWithName::new(Fragment::internal("test"), ColumnBuffer::uint1(vec![value]))])
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
		// against the already-committed tail - mirrors how the subscription
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
