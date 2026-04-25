// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use dashmap::DashMap;
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::id::{SeriesId, TableId},
};

use crate::{
	bucket::BucketId,
	snapshot::{Snapshot, SnapshotId, SnapshotMeta, SnapshotSource},
};

// Process-local snapshot registry. Three `DashMap`s:
// - `snapshots` - primary store keyed by `SnapshotId`.
// - `latest_table` - per-`TableId`, the highest `CommitVersion` materialized.
// - `latest_series` - per-`SeriesId`, the newest closed `BucketId`.
//
// Cheap to clone (`Arc`-backed). Lock-free reads. Writers are the two
// materialization actors; they write to disjoint primary-key spaces, so there
// is no shard contention between them. `insert` is atomic at the `DashMap`
// per-key granularity - readers holding `Arc<Snapshot>` observe either the
// old or the new entry, never a partial state, so bucket replacement is safe
// under concurrent reads.
#[derive(Clone)]
pub struct SnapshotRegistry {
	snapshots: Arc<DashMap<SnapshotId, Arc<Snapshot>>>,
	latest_table: Arc<DashMap<TableId, CommitVersion>>,
	latest_series: Arc<DashMap<SeriesId, BucketId>>,
}

impl SnapshotRegistry {
	pub fn new() -> Self {
		Self {
			snapshots: Arc::new(DashMap::new()),
			latest_table: Arc::new(DashMap::new()),
			latest_series: Arc::new(DashMap::new()),
		}
	}

	// Insert (or replace) a snapshot. Secondary indices are updated
	// monotonically: `latest_table` only advances, `latest_series` tracks the
	// newest-seen bucket id per series.
	pub fn insert(&self, snapshot: Arc<Snapshot>) {
		let id = snapshot.id;
		match &snapshot.source {
			SnapshotSource::Table {
				table_id,
				commit_version,
			} => {
				self.latest_table
					.entry(*table_id)
					.and_modify(|cv| {
						if *commit_version > *cv {
							*cv = *commit_version;
						}
					})
					.or_insert(*commit_version);
			}
			SnapshotSource::Series {
				series_id,
				bucket,
				..
			} => {
				let bid = bucket.id();
				self.latest_series
					.entry(*series_id)
					.and_modify(|b| {
						if bid > *b {
							*b = bid;
						}
					})
					.or_insert(bid);
			}
		}
		self.snapshots.insert(id, snapshot);
	}

	pub fn get(&self, id: &SnapshotId) -> Option<Arc<Snapshot>> {
		self.snapshots.get(id).map(|e| Arc::clone(e.value()))
	}

	pub fn latest_table(&self, table_id: TableId) -> Option<Arc<Snapshot>> {
		let cv = *self.latest_table.get(&table_id)?;
		self.get(&SnapshotId::Table {
			table_id,
			commit_version: cv,
		})
	}

	pub fn latest_series_bucket(&self, series_id: SeriesId) -> Option<Arc<Snapshot>> {
		let bucket = *self.latest_series.get(&series_id)?;
		self.get(&SnapshotId::Series {
			series_id,
			bucket,
		})
	}

	pub fn series_bucket(&self, series_id: SeriesId, bucket: BucketId) -> Option<Arc<Snapshot>> {
		self.get(&SnapshotId::Series {
			series_id,
			bucket,
		})
	}

	// Closed buckets for a series, in ascending order. Scans the primary map;
	// O(snapshots) but fine for v1 usage (listing is not on the hot path).
	pub fn series_buckets(&self, series_id: SeriesId) -> Vec<BucketId> {
		let mut buckets: Vec<BucketId> = self
			.snapshots
			.iter()
			.filter_map(|e| match e.key() {
				SnapshotId::Series {
					series_id: sid,
					bucket,
				} if *sid == series_id => Some(*bucket),
				_ => None,
			})
			.collect();
		buckets.sort();
		buckets
	}

	pub fn list(&self) -> Vec<SnapshotMeta> {
		self.snapshots.iter().map(|e| e.value().meta()).collect()
	}

	pub fn len(&self) -> usize {
		self.snapshots.len()
	}

	pub fn is_empty(&self) -> bool {
		self.snapshots.is_empty()
	}
}

impl Default for SnapshotRegistry {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use reifydb_core::interface::catalog::id::{SeriesId, TableId};
	use reifydb_runtime::context::clock::Clock;

	use super::*;
	use crate::{
		bucket::Bucket,
		snapshot::{ColumnBlock, Snapshot},
	};

	fn empty_block() -> ColumnBlock {
		ColumnBlock::new(Arc::new(vec![]), vec![])
	}

	fn mktable_snapshot(table_id: TableId, cv: CommitVersion) -> Snapshot {
		Snapshot {
			id: SnapshotId::Table {
				table_id,
				commit_version: cv,
			},
			source: SnapshotSource::Table {
				table_id,
				commit_version: cv,
			},
			namespace: "test".into(),
			name: "t".into(),
			created_at: Clock::Real.instant(),
			block: empty_block(),
		}
	}

	fn mkseries_snapshot(series_id: SeriesId, bucket: Bucket, seq: u64) -> Snapshot {
		Snapshot {
			id: SnapshotId::Series {
				series_id,
				bucket: bucket.id(),
			},
			source: SnapshotSource::Series {
				series_id,
				bucket,
				sequence_counter: seq,
			},
			namespace: "test".into(),
			name: "s".into(),
			created_at: Clock::Real.instant(),
			block: empty_block(),
		}
	}

	#[test]
	fn inserts_and_retrieves_table_snapshots() {
		let r = SnapshotRegistry::new();
		r.insert(Arc::new(mktable_snapshot(TableId(1), CommitVersion(5))));
		r.insert(Arc::new(mktable_snapshot(TableId(1), CommitVersion(10))));
		assert_eq!(r.len(), 2);
		let latest = r.latest_table(TableId(1)).unwrap();
		assert!(matches!(
			latest.id,
			SnapshotId::Table {
				commit_version: CommitVersion(10),
				..
			}
		));
	}

	#[test]
	fn latest_table_tracks_highest_commit_version() {
		let r = SnapshotRegistry::new();
		r.insert(Arc::new(mktable_snapshot(TableId(1), CommitVersion(10))));
		r.insert(Arc::new(mktable_snapshot(TableId(1), CommitVersion(5))));
		let latest = r.latest_table(TableId(1)).unwrap();
		assert!(matches!(
			latest.id,
			SnapshotId::Table {
				commit_version: CommitVersion(10),
				..
			}
		));
	}

	#[test]
	fn series_bucket_replacement_overwrites_atomically() {
		let r = SnapshotRegistry::new();
		let bucket = Bucket {
			start: 0,
			end: 100,
			width: 100,
		};
		r.insert(Arc::new(mkseries_snapshot(SeriesId(1), bucket, 5)));
		r.insert(Arc::new(mkseries_snapshot(SeriesId(1), bucket, 10)));
		assert_eq!(r.len(), 1, "replacement should not grow registry");
		let snap = r.series_bucket(SeriesId(1), bucket.id()).unwrap();
		let SnapshotSource::Series {
			sequence_counter,
			..
		} = snap.source
		else {
			panic!();
		};
		assert_eq!(sequence_counter, 10);
	}

	#[test]
	fn series_buckets_returns_ordered_list() {
		let r = SnapshotRegistry::new();
		for start in [0u64, 200, 100, 300] {
			let bucket = Bucket {
				start,
				end: start + 100,
				width: 100,
			};
			r.insert(Arc::new(mkseries_snapshot(SeriesId(7), bucket, 1)));
		}
		let buckets = r.series_buckets(SeriesId(7));
		assert_eq!(buckets, vec![BucketId(0), BucketId(100), BucketId(200), BucketId(300)]);
	}

	#[test]
	fn latest_series_bucket_returns_newest() {
		let r = SnapshotRegistry::new();
		let b1 = Bucket {
			start: 0,
			end: 100,
			width: 100,
		};
		let b2 = Bucket {
			start: 100,
			end: 200,
			width: 100,
		};
		r.insert(Arc::new(mkseries_snapshot(SeriesId(9), b1, 1)));
		r.insert(Arc::new(mkseries_snapshot(SeriesId(9), b2, 2)));
		let latest = r.latest_series_bucket(SeriesId(9)).unwrap();
		assert!(matches!(
			latest.id,
			SnapshotId::Series {
				bucket: BucketId(100),
				..
			}
		));
	}
}
