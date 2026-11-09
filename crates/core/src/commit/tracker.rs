// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeSet, HashMap},
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::value::partition::Partition;

use crate::{common::CommitVersion, delta::Delta, interface::catalog::storage::StorageId, key::any::TaggedKey};

struct Tracked {
	refs: usize,
	seed: CommitVersion,
	dropped: CommitVersion,
	writes: HashMap<Partition, CommitVersion>,
	order: BTreeSet<(CommitVersion, Partition)>,
}

impl Tracked {
	fn write(&mut self, partition: Partition, version: CommitVersion, entries: usize) {
		match self.writes.get(&partition).copied() {
			Some(held) if held >= version => return,
			Some(held) => {
				self.order.remove(&(held, partition));
			}
			None => {}
		}
		self.writes.insert(partition, version);
		self.order.insert((version, partition));
		while self.writes.len() > entries {
			let Some((oldest, evicted)) = self.order.pop_first() else {
				break;
			};
			self.writes.remove(&evicted);
			self.dropped = self.dropped.max(oldest);
		}
	}

	fn last_write(&self, partition: Partition) -> CommitVersion {
		let floor = self.seed.max(self.dropped);
		self.writes.get(&partition).map_or(floor, |version| (*version).max(floor))
	}
}

struct TrackerInner {
	received: CommitVersion,
	storages: HashMap<StorageId, Tracked>,
}

struct TrackerShared {
	inner: Mutex<TrackerInner>,
	frontier: AtomicU64,
	entries: usize,
}

#[derive(Clone)]
pub struct WriteTracker {
	shared: Arc<TrackerShared>,
}

impl WriteTracker {
	pub fn new(entries: usize, seed: CommitVersion) -> Self {
		Self {
			shared: Arc::new(TrackerShared {
				inner: Mutex::new(TrackerInner {
					received: seed,
					storages: HashMap::new(),
				}),
				frontier: AtomicU64::new(seed.0),
				entries: entries.max(1),
			}),
		}
	}

	pub fn detached() -> Self {
		Self::new(1, CommitVersion(0))
	}

	pub fn register(&self, storage: StorageId) -> TrackerLease {
		let mut inner = self.shared.inner.lock();
		let received = inner.received;
		let tracked = inner.storages.entry(storage).or_insert_with(|| Tracked {
			refs: 0,
			seed: received,
			dropped: CommitVersion(0),
			writes: HashMap::new(),
			order: BTreeSet::new(),
		});
		tracked.refs += 1;
		TrackerLease {
			tracker: self.clone(),
			storage,
		}
	}

	pub(crate) fn record(&self, version: CommitVersion, deltas: &[Delta]) {
		let mut inner = self.shared.inner.lock();
		inner.received = inner.received.max(version);
		if inner.storages.is_empty() {
			return;
		}
		let entries = self.shared.entries;
		for delta in deltas {
			let (storage, partition) = match delta.key() {
				TaggedKey::PartitionedRow(key) => (key.storage, Some(key.partition)),
				TaggedKey::Row(key) => (key.storage, None),
				TaggedKey::SeriesRow(key) => (key.storage, None),
				TaggedKey::PartitionedSeriesRow(key) => (key.storage, None),
				TaggedKey::SortedViewRow(key) => (key.storage, None),
				TaggedKey::PartitionedSortedViewRow(key) => (key.storage, None),
				_ => continue,
			};
			let Some(tracked) = inner.storages.get_mut(&storage) else {
				continue;
			};
			match partition {
				Some(partition) => tracked.write(partition, version, entries),
				None => tracked.dropped = tracked.dropped.max(version),
			}
		}
	}

	pub(crate) fn release(&self, version: CommitVersion) {
		self.shared.frontier.fetch_max(version.0, Ordering::Release);
	}

	fn unregister(&self, storage: StorageId) {
		let mut inner = self.shared.inner.lock();
		let Some(tracked) = inner.storages.get_mut(&storage) else {
			return;
		};
		tracked.refs -= 1;
		if tracked.refs == 0 {
			inner.storages.remove(&storage);
		}
	}
}

pub struct TrackerLease {
	tracker: WriteTracker,
	storage: StorageId,
}

impl TrackerLease {
	pub fn frontier(&self) -> CommitVersion {
		CommitVersion(self.tracker.shared.frontier.load(Ordering::Acquire))
	}

	pub fn last_writes(&self, partitions: &[Partition]) -> Vec<CommitVersion> {
		let inner = self.tracker.shared.inner.lock();
		let tracked = inner.storages.get(&self.storage).unwrap_or_else(|| {
			panic!("a held lease on storage {:?} must keep its registration", self.storage)
		});
		partitions.iter().map(|partition| tracked.last_write(*partition)).collect()
	}
}

impl Drop for TrackerLease {
	fn drop(&mut self) {
		self.tracker.unregister(self.storage);
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::row::bytes::EncodedBytes;
	use reifydb_value::{util::cowvec::CowVec, value::row_number::RowNumber};

	use super::*;
	use crate::{
		delta::SetPre,
		interface::catalog::id::RingBufferId,
		key::row::{PartitionedRowKey, RowKey},
	};

	const VAULT: StorageId = StorageId::RingBuffer(RingBufferId(7));

	fn set(storage: StorageId, partition: u128) -> Delta {
		Delta::Set {
			key: PartitionedRowKey::new(storage, Partition(partition), RowNumber(1)).into(),
			bytes: EncodedBytes(CowVec::new(Vec::new())),
			pre: SetPre::Absent,
		}
	}

	fn v(version: u64) -> CommitVersion {
		CommitVersion(version)
	}

	#[test]
	fn a_write_after_the_read_is_reported_for_its_partition_only() {
		// a cached row is only reusable while no write to its partition sits above the read version.
		let tracker = WriteTracker::new(16, v(10));
		let lease = tracker.register(VAULT);
		tracker.record(v(11), &[set(VAULT, 1)]);
		tracker.release(v(11));
		assert_eq!(lease.last_writes(&[Partition(1), Partition(2)]), vec![v(11), v(10)]);
		assert_eq!(lease.frontier(), v(11));
	}

	#[test]
	fn an_out_of_order_older_write_never_lowers_the_last_write() {
		// the last write must be the max seen, or a later write would be forgotten.
		let tracker = WriteTracker::new(16, v(0));
		let lease = tracker.register(VAULT);
		tracker.record(v(12), &[set(VAULT, 1)]);
		tracker.record(v(11), &[set(VAULT, 1)]);
		assert_eq!(lease.last_writes(&[Partition(1)]), vec![v(12)]);
	}

	#[test]
	fn a_silent_remove_counts_as_a_write() {
		// a ttl eviction empties the lookup result, so it must invalidate like any write.
		let tracker = WriteTracker::new(16, v(0));
		let lease = tracker.register(VAULT);
		let key = PartitionedRowKey::new(VAULT, Partition(3), RowNumber(1)).into();
		tracker.record(v(5), &[Delta::remove_silent(key)]);
		assert_eq!(lease.last_writes(&[Partition(3)]), vec![v(5)]);
	}

	#[test]
	fn an_evicted_partition_is_never_reported_older_than_its_write() {
		// eviction must raise the floor, otherwise a dropped write reads as no write at all.
		let tracker = WriteTracker::new(2, v(0));
		let lease = tracker.register(VAULT);
		tracker.record(v(100), &[set(VAULT, 1)]);
		tracker.record(v(300), &[set(VAULT, 2)]);
		tracker.record(v(500), &[set(VAULT, 3)]);
		assert_eq!(
			lease.last_writes(&[Partition(1), Partition(2), Partition(9)]),
			vec![v(100), v(300), v(100)]
		);
	}

	#[test]
	fn a_registration_never_trusts_a_version_received_before_it() {
		// a parked write received before registration was never recorded, so it must count as unknown.
		let tracker = WriteTracker::new(16, v(0));
		tracker.record(v(40), &[set(VAULT, 1)]);
		let lease = tracker.register(VAULT);
		assert_eq!(lease.last_writes(&[Partition(1), Partition(2)]), vec![v(40), v(40)]);
	}

	#[test]
	fn a_row_key_without_a_partition_counts_for_every_partition() {
		// a write the tracker cannot place must make every partition of that storage look written.
		let tracker = WriteTracker::new(16, v(0));
		let lease = tracker.register(VAULT);
		tracker.record(v(9), &[Delta::remove_silent(RowKey::new(VAULT, RowNumber(4)).into())]);
		assert_eq!(lease.last_writes(&[Partition(1), Partition(2)]), vec![v(9), v(9)]);
	}

	#[test]
	fn an_unregistered_storage_is_not_tracked_and_comes_back_with_a_fresh_seed() {
		// writes while nobody holds a lease are not recorded, so a new lease must start above them.
		let tracker = WriteTracker::new(16, v(0));
		let other = StorageId::RingBuffer(RingBufferId(8));
		let first = tracker.register(VAULT);
		let second = tracker.register(VAULT);
		tracker.record(v(3), &[set(VAULT, 1), set(other, 1)]);
		drop(first);
		assert_eq!(second.last_writes(&[Partition(1)]), vec![v(3)]);
		drop(second);
		tracker.record(v(7), &[set(VAULT, 2)]);
		let again = tracker.register(VAULT);
		assert_eq!(again.last_writes(&[Partition(1), Partition(2)]), vec![v(7), v(7)]);
	}

	#[test]
	fn a_detached_tracker_never_moves_its_frontier() {
		// with no commit queue there is no write feed, so no read may ever be served from a cache.
		let tracker = WriteTracker::detached();
		let lease = tracker.register(VAULT);
		assert_eq!(lease.frontier(), v(0));
	}
}
