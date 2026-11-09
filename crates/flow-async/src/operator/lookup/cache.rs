// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{BTreeMap, HashMap};

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{commit::tracker::TrackerLease, common::CommitVersion};
use reifydb_value::value::{partition::Partition, row_number::RowNumber};

use crate::transaction::LookupAt;

pub(crate) type CachedRow = Option<(RowNumber, EncodedBytes)>;

struct Cached {
	stamp: u64,
	version: CommitVersion,
	row: CachedRow,
}

pub(crate) struct LookupCache {
	entries: usize,
	rows: HashMap<Partition, Cached>,
	order: BTreeMap<u64, Partition>,
	next: u64,
}

impl LookupCache {
	pub(crate) fn new(entries: usize) -> Self {
		Self {
			entries: entries.max(1),
			rows: HashMap::new(),
			order: BTreeMap::new(),
			next: 0,
		}
	}

	pub(crate) fn hits(
		&self,
		lease: &TrackerLease,
		at: LookupAt,
		floor: Option<CommitVersion>,
		partitions: &[Partition],
	) -> Vec<Option<CachedRow>> {
		let (read, seen) = match at {
			LookupAt::Commit(wanted) => (wanted, wanted),
			LookupAt::Layered {
				base,
				commit,
				..
			} => (base, commit),
		};
		let mut hits = vec![None; partitions.len()];
		if floor.is_some_and(|floor| read < floor) {
			return hits;
		}
		let frontier = lease.frontier();
		let held: Vec<(usize, &Cached)> = partitions
			.iter()
			.enumerate()
			.filter_map(|(at, partition)| self.rows.get(partition).map(|cached| (at, cached)))
			.filter(|(_, cached)| cached.version.max(seen) <= frontier)
			.collect();
		if held.is_empty() {
			return hits;
		}
		let asked: Vec<Partition> = held.iter().map(|(at, _)| partitions[*at]).collect();
		for ((at, cached), last) in held.into_iter().zip(lease.last_writes(&asked)) {
			if last <= cached.version.min(read) {
				hits[at] = Some(cached.row.clone());
			}
		}
		hits
	}

	pub(crate) fn store(&mut self, version: CommitVersion, partition: Partition, row: CachedRow) {
		if let Some(held) = self.rows.get(&partition) {
			if held.version >= version {
				return;
			}
			self.order.remove(&held.stamp);
		}
		let stamp = self.next;
		self.next += 1;
		self.order.insert(stamp, partition);
		self.rows.insert(
			partition,
			Cached {
				stamp,
				version,
				row,
			},
		);
		while self.rows.len() > self.entries {
			let Some((_, oldest)) = self.order.pop_first() else {
				break;
			};
			self.rows.remove(&oldest);
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		commit::queue::CommitQueue,
		common::{ChangeVersion, SourceVersion},
		delta::{Delta, SetPre},
		event::{EventListener, transaction::PostCommitEvent},
		interface::catalog::{
			id::{RingBufferId, ViewId},
			storage::StorageId,
		},
		key::row::PartitionedRowKey,
	};
	use reifydb_value::{byte_size::ByteSize, util::cowvec::CowVec, value::datetime::DateTime};
	use reifydb_wal::lsn::Lsn;

	use super::*;

	const VAULT: StorageId = StorageId::RingBuffer(RingBufferId(7));

	fn v(version: u64) -> CommitVersion {
		CommitVersion(version)
	}

	fn at(version: u64) -> LookupAt {
		LookupAt::Commit(v(version))
	}

	fn layered(base: u64, commit: u64) -> LookupAt {
		LookupAt::Layered {
			view: ViewId(1),
			base: v(base),
			commit: v(commit),
			through: SourceVersion(base),
		}
	}

	fn row(byte: u8) -> CachedRow {
		Some((RowNumber(1), EncodedBytes(CowVec::new(vec![byte]))))
	}

	fn queue() -> CommitQueue {
		CommitQueue::new(ByteSize::from_mib(1), false, v(10), 16)
	}

	fn commit(queue: &CommitQueue, version: u64, partitions: &[u128]) {
		let deltas = partitions
			.iter()
			.map(|partition| Delta::Set {
				key: PartitionedRowKey::new(VAULT, Partition(*partition), RowNumber(1)).into(),
				bytes: EncodedBytes(CowVec::new(vec![0])),
				pre: SetPre::Absent,
			})
			.collect();
		let at = ChangeVersion {
			commit: v(version),
			source: v(version).into(),
		};
		queue.on(&PostCommitEvent::new(
			CowVec::new(deltas),
			at,
			Lsn::new(version),
			DateTime::default(),
			CowVec::new(Vec::new()),
		));
	}

	fn commits(queue: &CommitQueue, through: u64, writes: &[(u64, u128)]) {
		for version in 11..=through {
			let partitions: Vec<u128> = writes
				.iter()
				.filter(|(at, _)| *at == version)
				.map(|(_, partition)| *partition)
				.collect();
			commit(queue, version, &partitions);
		}
	}

	#[test]
	fn a_hit_returns_the_row_read_before_when_nothing_was_written_since() {
		// the cache may only answer with exactly what a storage read at the wanted version returns.
		let queue = queue();
		let lease = queue.write_tracker().register(VAULT);
		let mut cache = LookupCache::new(16);
		commits(&queue, 20, &[(15, 9)]);
		cache.store(v(12), Partition(1), row(4));
		assert_eq!(cache.hits(&lease, at(18), None, &[Partition(1)]), vec![Some(row(4))]);
	}

	#[test]
	fn a_write_between_the_read_and_the_wanted_version_forces_a_fresh_read() {
		// a right update between two left rows must reach the second one.
		let queue = queue();
		let lease = queue.write_tracker().register(VAULT);
		let mut cache = LookupCache::new(16);
		commits(&queue, 20, &[(15, 1)]);
		cache.store(v(12), Partition(1), row(4));
		assert_eq!(cache.hits(&lease, at(18), None, &[Partition(1)]), vec![None]);
	}

	#[test]
	fn a_wanted_version_older_than_the_read_needs_no_write_after_it() {
		// reading backwards is only safe when the partition did not change between the two versions.
		let queue = queue();
		let lease = queue.write_tracker().register(VAULT);
		let mut cache = LookupCache::new(16);
		commits(&queue, 20, &[(14, 1)]);
		cache.store(v(18), Partition(1), row(4));
		assert_eq!(cache.hits(&lease, at(12), None, &[Partition(1)]), vec![None]);
		assert_eq!(cache.hits(&lease, at(16), None, &[Partition(1)]), vec![Some(row(4))]);
	}

	#[test]
	fn a_version_above_the_frontier_is_never_served() {
		// the tracker has not seen every commit above its frontier, so a write may be missing there.
		let queue = queue();
		let lease = queue.write_tracker().register(VAULT);
		let mut cache = LookupCache::new(16);
		commits(&queue, 12, &[]);
		commit(&queue, 14, &[2]);
		cache.store(v(11), Partition(1), row(4));
		assert_eq!(cache.hits(&lease, at(12), None, &[Partition(1)]), vec![Some(row(4))]);
		assert_eq!(cache.hits(&lease, at(14), None, &[Partition(1)]), vec![None]);
	}

	#[test]
	fn a_parked_write_already_blocks_reads_older_than_it() {
		// a write must count once received, or a read below it could be served after it lands.
		let queue = queue();
		let lease = queue.write_tracker().register(VAULT);
		let mut cache = LookupCache::new(16);
		commits(&queue, 12, &[]);
		commit(&queue, 14, &[1]);
		cache.store(v(11), Partition(1), row(4));
		assert_eq!(cache.hits(&lease, at(12), None, &[Partition(1)]), vec![None]);
	}

	#[test]
	fn a_version_below_the_lease_floor_goes_to_storage() {
		// a read below the floor must reach the storage read, which refuses it loudly.
		let queue = queue();
		let lease = queue.write_tracker().register(VAULT);
		let mut cache = LookupCache::new(16);
		commits(&queue, 20, &[]);
		cache.store(v(11), Partition(1), row(4));
		assert_eq!(cache.hits(&lease, at(12), Some(v(13)), &[Partition(1)]), vec![None]);
		assert_eq!(cache.hits(&lease, at(13), Some(v(13)), &[Partition(1)]), vec![Some(row(4))]);
	}

	#[test]
	fn a_full_cache_drops_its_oldest_read() {
		// the cap must hold, and an evicted row must be read again rather than guessed.
		let queue = queue();
		let lease = queue.write_tracker().register(VAULT);
		let mut cache = LookupCache::new(2);
		commits(&queue, 20, &[]);
		cache.store(v(11), Partition(1), row(1));
		cache.store(v(11), Partition(2), row(2));
		cache.store(v(11), Partition(3), row(3));
		assert_eq!(
			cache.hits(&lease, at(19), None, &[Partition(1), Partition(2), Partition(3)]),
			vec![None, Some(row(2)), Some(row(3))]
		);
	}

	#[test]
	fn a_layered_read_of_a_partition_its_commit_never_wrote_is_served_from_the_cache() {
		// a commit that never wrote the partition must leave the layered read equal to the read at base.
		let queue = queue();
		let lease = queue.write_tracker().register(VAULT);
		let mut cache = LookupCache::new(16);
		commits(&queue, 20, &[(15, 2)]);
		cache.store(v(12), Partition(1), row(4));
		assert_eq!(cache.hits(&lease, layered(14, 15), None, &[Partition(1)]), vec![Some(row(4))]);
	}

	#[test]
	fn a_layered_read_of_a_partition_its_commit_wrote_goes_to_storage() {
		// a write inside the commit must never be hidden by a cached read taken above it.
		let queue = queue();
		let lease = queue.write_tracker().register(VAULT);
		let mut cache = LookupCache::new(16);
		commits(&queue, 20, &[(15, 1)]);
		cache.store(v(17), Partition(1), row(4));
		assert_eq!(cache.hits(&lease, layered(14, 15), None, &[Partition(1)]), vec![None]);
	}

	#[test]
	fn a_layered_read_whose_commit_is_above_the_frontier_goes_to_storage() {
		// a commit the tracker has not received may hold a write to the partition, so it must never be served.
		let queue = queue();
		let lease = queue.write_tracker().register(VAULT);
		let mut cache = LookupCache::new(16);
		commits(&queue, 14, &[]);
		cache.store(v(12), Partition(1), row(4));
		assert_eq!(cache.hits(&lease, layered(14, 15), None, &[Partition(1)]), vec![None]);
	}

	#[test]
	fn a_layered_read_whose_base_is_below_the_floor_goes_to_storage() {
		// a base below the floor must reach the storage read, which refuses it loudly.
		let queue = queue();
		let lease = queue.write_tracker().register(VAULT);
		let mut cache = LookupCache::new(16);
		commits(&queue, 20, &[]);
		cache.store(v(11), Partition(1), row(4));
		assert_eq!(cache.hits(&lease, layered(14, 15), Some(v(15)), &[Partition(1)]), vec![None]);
		assert_eq!(cache.hits(&lease, layered(15, 16), Some(v(15)), &[Partition(1)]), vec![Some(row(4))]);
	}
}
