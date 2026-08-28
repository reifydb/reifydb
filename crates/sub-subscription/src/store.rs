// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, VecDeque},
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use dashmap::{DashMap, DashSet};
use reifydb_core::interface::{catalog::id::SubscriptionId, change::StagedBatch};
use reifydb_runtime::sync::{
	mutex::Mutex,
	rwlock::{RwLock, RwLockReadGuard},
};
use reifydb_value::reifydb_assertions;
use tokio::sync::Notify;
use tracing::instrument;

struct SubscriptionBuffer {
	queue: VecDeque<StagedBatch>,
	capacity: usize,
	overrun: Option<u16>,

	column_names: Vec<String>,
}

fn saturating_u16(value: usize) -> u16 {
	value.min(u16::MAX as usize) as u16
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
			assert_ne!(raw, 0,"the subscription id counter wrapped past u64::MAX and issued 0, so a new subscriber collides with the reserved initial id and would receive another subscriber's delivery stream (issued={})", raw);
		}
		SubscriptionId(raw)
	}

	pub fn register(&self, id: SubscriptionId, column_names: Vec<String>) {
		self.inner.insert(
			id,
			SubscriptionBuffer {
				queue: VecDeque::with_capacity(self.default_capacity),
				capacity: self.default_capacity,
				overrun: None,
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

	pub fn drain(&self, id: &SubscriptionId, max_batches: usize) -> Vec<StagedBatch> {
		match self.inner.get_mut(id) {
			Some(mut buf) => {
				let count = max_batches.min(buf.queue.len());
				buf.queue.drain(..count).collect()
			}
			None => Vec::new(),
		}
	}

	pub fn drain_into(&self, id: &SubscriptionId, max_batches: usize, out: &mut Vec<StagedBatch>) {
		if let Some(mut buf) = self.inner.get_mut(id) {
			let count = max_batches.min(buf.queue.len());
			out.extend(buf.queue.drain(..count));
		}
	}

	pub fn overrun(&self, id: &SubscriptionId) -> Option<u16> {
		self.inner.get(id).and_then(|buf| buf.overrun)
	}

	pub fn capacity(&self, id: &SubscriptionId) -> usize {
		self.inner.get(id).map(|buf| buf.capacity).unwrap_or(0)
	}

	pub fn active_subscriptions(&self) -> Vec<SubscriptionId> {
		self.inner.iter().map(|entry| *entry.key()).collect()
	}

	#[instrument(name = "subscription::commit_staged", level = "debug", skip(self, staged), fields(subs = staged.len()))]
	pub fn commit_staged(&self, staged: HashMap<SubscriptionId, Vec<StagedBatch>>) {
		if staged.is_empty() {
			return;
		}
		self.append_staged_under_coord(staged);
		self.notify_wakers();
	}

	#[inline]
	fn append_staged_under_coord(&self, staged: HashMap<SubscriptionId, Vec<StagedBatch>>) {
		let _write = self.coord.write();
		for (id, columns_vec) in staged {
			let Some(mut buf) = self.inner.get_mut(&id) else {
				continue;
			};
			let staged_count = columns_vec.len();
			if let Some(overran) = buf.overrun {
				buf.overrun = Some(overran.saturating_add(saturating_u16(staged_count)));
				continue;
			}
			for (idx, batch) in columns_vec.into_iter().enumerate() {
				reifydb_assertions! {
					let cap = buf.capacity;
					assert!(
						cap != 0,
						"subscription {:?} ring buffer has zero capacity, so the lag branch can never run and push_back grows the queue without bound, leaking memory per committed batch",
						id
					);
				}
				if buf.queue.len() >= buf.capacity {
					let discarded = buf.queue.len() + (staged_count - idx);
					buf.queue.clear();
					buf.overrun = Some(saturating_u16(discarded));
					break;
				}
				buf.queue.push_back(batch);
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
	use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
	use reifydb_value::{fragment::Fragment, value::diff_type::DiffType};

	use super::*;

	fn test_columns(value: u8) -> Columns {
		Columns::new(vec![ColumnWithName::new(Fragment::internal("test"), ColumnBuffer::uint1(vec![value]))])
	}

	fn stage(id: SubscriptionId, values: &[u8]) -> HashMap<SubscriptionId, Vec<StagedBatch>> {
		let mut map = HashMap::new();
		map.insert(id, values.iter().copied().map(|v| (DiffType::Insert, test_columns(v))).collect());
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
	fn commit_staged_past_capacity_marks_the_subscription_lagged() {
		// Separate commits so each push evaluates capacity against the committed tail, as the CDC consumer
		// drives it.
		let store = SubscriptionStore::new(2);
		let id = store.next_id();
		store.register(id, vec!["test".to_string()]);

		store.commit_staged(stage(id, &[1]));
		store.commit_staged(stage(id, &[2]));
		assert_eq!(store.overrun(&id), None, "a buffer exactly at capacity has not lost anything yet");

		store.commit_staged(stage(id, &[3]));

		assert_eq!(
			store.overrun(&id),
			Some(3),
			"overflowing must terminate the subscription and record how far past capacity it went"
		);
	}

	#[test]
	fn a_lagged_subscription_drops_its_queued_batches() {
		// Delivering a partial prefix would leave the subscriber holding a state the server never had.
		let store = SubscriptionStore::new(2);
		let id = store.next_id();
		store.register(id, vec!["test".to_string()]);

		store.commit_staged(stage(id, &[1]));
		store.commit_staged(stage(id, &[2]));
		store.commit_staged(stage(id, &[3]));

		assert!(
			store.drain(&id, 10).is_empty(),
			"a lagged subscription must surrender its queue, not a prefix of it"
		);
	}

	#[test]
	fn a_lagged_subscription_accepts_no_further_batches() {
		// The state is terminal, so a later commit must not refill a queue the subscriber will never read.
		let store = SubscriptionStore::new(2);
		let id = store.next_id();
		store.register(id, vec!["test".to_string()]);

		store.commit_staged(stage(id, &[1]));
		store.commit_staged(stage(id, &[2]));
		store.commit_staged(stage(id, &[3]));
		store.commit_staged(stage(id, &[4]));

		assert!(store.overrun(&id).is_some(), "the overrun must never clear back to none");
		assert!(store.drain(&id, 10).is_empty(), "no batch may be queued after the subscription is lagged");
	}

	#[test]
	fn a_subscription_under_capacity_is_never_lagged() {
		// The common path must be untouched by the lag branch.
		let store = SubscriptionStore::new(16);
		let id = store.next_id();
		store.register(id, vec!["test".to_string()]);

		store.commit_staged(stage(id, &[1, 2, 3]));

		assert_eq!(store.overrun(&id), None);
		assert_eq!(store.drain(&id, 10).len(), 3);
	}

	#[test]
	fn overrun_counts_every_batch_the_lag_discarded() {
		// A count of one would read as a single hiccup rather than the whole queue being surrendered.
		let store = SubscriptionStore::new(2);
		let id = store.next_id();
		store.register(id, vec!["test".to_string()]);

		store.commit_staged(stage(id, &[1]));
		store.commit_staged(stage(id, &[2]));
		assert_eq!(store.overrun(&id), None, "a buffer at capacity has not discarded anything yet");

		store.commit_staged(stage(id, &[3, 4, 5]));

		assert_eq!(store.overrun(&id), Some(5), "two queued batches plus three refused must all be counted");
	}

	#[test]
	fn overrun_keeps_accumulating_while_the_subscription_stays_lagged() {
		// The count sizes a future mailbox, so it must reflect the whole shortfall, not just the first
		// overflow.
		let store = SubscriptionStore::new(2);
		let id = store.next_id();
		store.register(id, vec!["test".to_string()]);

		store.commit_staged(stage(id, &[1]));
		store.commit_staged(stage(id, &[2]));
		store.commit_staged(stage(id, &[3]));
		assert_eq!(store.overrun(&id), Some(3));

		store.commit_staged(stage(id, &[4, 5]));

		assert_eq!(store.overrun(&id), Some(5), "batches refused after the lag must be counted too");
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
