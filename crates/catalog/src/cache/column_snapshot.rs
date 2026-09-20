// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		column_snapshot::{ColumnSnapshot, ColumnSnapshotSource},
		id::{ColumnSnapshotId, SeriesId, TableId},
	},
};

use reifydb_value::value::partition::Partition;

use crate::cache::{CatalogCache, MultiVersionColumnSnapshot};

impl CatalogCache {
	pub fn find_column_snapshot_at(&self, id: ColumnSnapshotId, version: CommitVersion) -> Option<ColumnSnapshot> {
		self.column_snapshots.get(&id).and_then(|entry| entry.value().get(version))
	}

	pub fn find_column_snapshot(&self, id: ColumnSnapshotId) -> Option<ColumnSnapshot> {
		self.column_snapshots.get(&id).and_then(|entry| entry.value().get_latest())
	}

	pub fn find_column_snapshot_for_series_bucket_at(
		&self,
		series_id: SeriesId,
		partition: Option<Partition>,
		bucket_start: u64,
		version: CommitVersion,
	) -> Option<ColumnSnapshot> {
		let entry = self.column_snapshots_for_series.get(&series_id)?;
		let buckets = entry.value();

		for (bs, p, snap_id) in buckets.iter() {
			if *bs == bucket_start
				&& *p == partition
				&& let Some(snap) = self.find_column_snapshot_at(*snap_id, version)
			{
				return Some(snap);
			}
		}
		None
	}

	pub fn find_latest_column_snapshot_for_table_at(
		&self,
		table_id: TableId,
		version: CommitVersion,
	) -> Option<ColumnSnapshot> {
		let entry = self.column_snapshots_for_table.get(&table_id)?;
		let map = entry.value();

		for (_cv, snap_id) in map.iter().rev() {
			if let Some(snap) = self.find_column_snapshot_at(*snap_id, version) {
				return Some(snap);
			}
		}
		None
	}

	pub fn list_column_snapshots_for_series_at(
		&self,
		series_id: SeriesId,
		version: CommitVersion,
	) -> Vec<ColumnSnapshot> {
		let Some(entry) = self.column_snapshots_for_series.get(&series_id) else {
			return Vec::new();
		};
		let buckets = entry.value();
		let mut out = Vec::with_capacity(buckets.len());
		for (_bs, _p, snap_id) in buckets.iter() {
			if let Some(snap) = self.find_column_snapshot_at(*snap_id, version) {
				out.push(snap);
			}
		}
		out
	}

	pub fn list_column_snapshots_for_series_partition_at(
		&self,
		series_id: SeriesId,
		partition: Partition,
		version: CommitVersion,
	) -> Vec<ColumnSnapshot> {
		let Some(entry) = self.column_snapshots_for_series.get(&series_id) else {
			return Vec::new();
		};
		let buckets = entry.value();
		let mut out = Vec::new();
		for (_bs, p, snap_id) in buckets.iter() {
			if p.unwrap_or_default() == partition
				&& let Some(snap) = self.find_column_snapshot_at(*snap_id, version)
			{
				out.push(snap);
			}
		}
		out
	}

	pub fn list_column_snapshots_for_table_at(
		&self,
		table_id: TableId,
		version: CommitVersion,
	) -> Vec<ColumnSnapshot> {
		let Some(entry) = self.column_snapshots_for_table.get(&table_id) else {
			return Vec::new();
		};
		let map = entry.value();
		let mut out = Vec::with_capacity(map.len());
		for (_cv, snap_id) in map.iter() {
			if let Some(snap) = self.find_column_snapshot_at(*snap_id, version) {
				out.push(snap);
			}
		}
		out
	}

	pub fn set_column_snapshot(
		&self,
		id: ColumnSnapshotId,
		version: CommitVersion,
		snapshot: Option<ColumnSnapshot>,
	) {
		let _guard = self.write_lock.lock();
		if let Some(entry) = self.column_snapshots.get(&id)
			&& let Some(prev) = entry.value().get_latest()
		{
			self.remove_secondary_index_entry(&prev);
		}

		let multi = self.column_snapshots.get_or_insert_with(id, MultiVersionColumnSnapshot::new);
		match snapshot {
			Some(new) => {
				self.insert_secondary_index_entry(&new);
				multi.value().insert(version, new);
			}
			None => {
				multi.value().remove(version);
			}
		}
	}

	fn insert_secondary_index_entry(&self, snap: &ColumnSnapshot) {
		match &snap.source {
			ColumnSnapshotSource::SeriesBucket {
				series_id,
				bucket_start,
				partition,
				..
			} => {
				let mut existing = self
					.column_snapshots_for_series
					.get(series_id)
					.map(|e| e.value().clone())
					.unwrap_or_default();
				existing.insert((*bucket_start, *partition, snap.id));
				self.column_snapshots_for_series.insert(*series_id, existing);
			}
			ColumnSnapshotSource::Table {
				table_id,
				commit_version,
			} => {
				let mut existing = self
					.column_snapshots_for_table
					.get(table_id)
					.map(|e| e.value().clone())
					.unwrap_or_default();
				existing.insert(*commit_version, snap.id);
				self.column_snapshots_for_table.insert(*table_id, existing);
			}
		}
	}

	fn remove_secondary_index_entry(&self, snap: &ColumnSnapshot) {
		match &snap.source {
			ColumnSnapshotSource::SeriesBucket {
				series_id,
				bucket_start,
				partition,
				..
			} => {
				if let Some(entry) = self.column_snapshots_for_series.get(series_id) {
					let mut updated = entry.value().clone();
					updated.remove(&(*bucket_start, *partition, snap.id));
					if updated.is_empty() {
						self.column_snapshots_for_series.remove(series_id);
					} else {
						self.column_snapshots_for_series.insert(*series_id, updated);
					}
				}
			}
			ColumnSnapshotSource::Table {
				table_id,
				commit_version,
			} => {
				if let Some(entry) = self.column_snapshots_for_table.get(table_id) {
					let mut updated = entry.value().clone();
					updated.remove(commit_version);
					if updated.is_empty() {
						self.column_snapshots_for_table.remove(table_id);
					} else {
						self.column_snapshots_for_table.insert(*table_id, updated);
					}
				}
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::catalog::id::NamespaceId;
	use reifydb_value::value::Value;

	use super::*;

	fn series_snap(id: u64, series_id: u64, bucket_start: u64, sealed_at: u64) -> ColumnSnapshot {
		ColumnSnapshot {
			id: ColumnSnapshotId(id),
			namespace: NamespaceId(1),
			source: ColumnSnapshotSource::SeriesBucket {
				series_id: SeriesId(series_id),
				bucket_start,
				bucket_width: 100,
				partition: None,
				sequence_counter: 0,
				sealed_at_commit_version: CommitVersion(sealed_at),
			},
			row_count: 0,
			partition_values: Vec::new(),
			stats: Vec::new(),
		}
	}

	fn partitioned_series_snap(id: u64, series_id: u64, bucket_start: u64, partition: Partition) -> ColumnSnapshot {
		ColumnSnapshot {
			id: ColumnSnapshotId(id),
			namespace: NamespaceId(1),
			source: ColumnSnapshotSource::SeriesBucket {
				series_id: SeriesId(series_id),
				bucket_start,
				bucket_width: 100,
				partition: Some(partition),
				sequence_counter: 0,
				sealed_at_commit_version: CommitVersion(1),
			},
			row_count: 0,
			partition_values: Vec::new(),
			stats: Vec::new(),
		}
	}

	fn table_snap(id: u64, table_id: u64, commit_version: u64) -> ColumnSnapshot {
		ColumnSnapshot {
			id: ColumnSnapshotId(id),
			namespace: NamespaceId(1),
			source: ColumnSnapshotSource::Table {
				table_id: TableId(table_id),
				commit_version: CommitVersion(commit_version),
			},
			row_count: 0,
			partition_values: Vec::new(),
			stats: Vec::new(),
		}
	}

	#[test]
	fn series_snapshots_listed_in_bucket_order() {
		let cat = CatalogCache::new();
		cat.set_column_snapshot(ColumnSnapshotId(1), CommitVersion(1), Some(series_snap(1, 7, 200, 1)));
		cat.set_column_snapshot(ColumnSnapshotId(2), CommitVersion(1), Some(series_snap(2, 7, 0, 1)));
		cat.set_column_snapshot(ColumnSnapshotId(3), CommitVersion(1), Some(series_snap(3, 7, 100, 1)));

		let all = cat.list_column_snapshots_for_series_at(SeriesId(7), CommitVersion(1));
		let starts: Vec<u64> = all
			.iter()
			.map(|s| match s.source {
				ColumnSnapshotSource::SeriesBucket {
					bucket_start,
					..
				} => bucket_start,
				_ => unreachable!(),
			})
			.collect();
		assert_eq!(starts, vec![0, 100, 200]);
	}

	#[test]
	fn find_for_series_bucket_returns_match() {
		let cat = CatalogCache::new();
		cat.set_column_snapshot(ColumnSnapshotId(1), CommitVersion(1), Some(series_snap(1, 7, 100, 1)));
		let found = cat
			.find_column_snapshot_for_series_bucket_at(SeriesId(7), None, 100, CommitVersion(1))
			.expect("should find");
		assert_eq!(found.id, ColumnSnapshotId(1));

		assert!(cat.find_column_snapshot_for_series_bucket_at(SeriesId(7), None, 999, CommitVersion(1)).is_none());
	}

	#[test]
	fn find_for_series_bucket_distinguishes_partitions() {
		// the cache is consulted before the store, so a cache that ignores the partition
		// hands back another partition's block while the store would have been right
		let cat = CatalogCache::new();
		let us = Partition::of(&[Value::Utf8("us".to_string())]);
		let eu = Partition::of(&[Value::Utf8("eu".to_string())]);
		cat.set_column_snapshot(
			ColumnSnapshotId(1),
			CommitVersion(1),
			Some(partitioned_series_snap(1, 7, 100, us)),
		);
		cat.set_column_snapshot(
			ColumnSnapshotId(2),
			CommitVersion(1),
			Some(partitioned_series_snap(2, 7, 100, eu)),
		);

		let found_us = cat
			.find_column_snapshot_for_series_bucket_at(SeriesId(7), Some(us), 100, CommitVersion(1))
			.expect("the us partition must be found");
		let found_eu = cat
			.find_column_snapshot_for_series_bucket_at(SeriesId(7), Some(eu), 100, CommitVersion(1))
			.expect("the eu partition must be found");
		assert_eq!(found_us.id, ColumnSnapshotId(1));
		assert_eq!(found_eu.id, ColumnSnapshotId(2));

		assert!(
			cat.find_column_snapshot_for_series_bucket_at(SeriesId(7), None, 100, CommitVersion(1))
				.is_none(),
			"an unpartitioned lookup must not match a partitioned bucket"
		);
	}

	#[test]
	fn partition_scoped_listing_reads_only_its_own_partition() {
		let cat = CatalogCache::new();
		let us = Partition::of(&[Value::Utf8("us".to_string())]);
		let eu = Partition::of(&[Value::Utf8("eu".to_string())]);
		cat.set_column_snapshot(
			ColumnSnapshotId(1),
			CommitVersion(1),
			Some(partitioned_series_snap(1, 7, 0, us)),
		);
		cat.set_column_snapshot(
			ColumnSnapshotId(2),
			CommitVersion(1),
			Some(partitioned_series_snap(2, 7, 100, us)),
		);
		cat.set_column_snapshot(
			ColumnSnapshotId(3),
			CommitVersion(1),
			Some(partitioned_series_snap(3, 7, 0, eu)),
		);

		let scoped = cat.list_column_snapshots_for_series_partition_at(SeriesId(7), us, CommitVersion(1));
		assert_eq!(scoped.len(), 2);
		assert_eq!(
			cat.list_column_snapshots_for_series_at(SeriesId(7), CommitVersion(1)).len(),
			3,
			"the unscoped listing still spans every partition"
		);
	}

	#[test]
	fn two_partitions_of_one_bucket_do_not_evict_each_other() {
		// the secondary index is keyed on the bucket, so without the partition the second
		// insert replaces the first and one partition's block becomes unreachable
		let cat = CatalogCache::new();
		let us = Partition::of(&[Value::Utf8("us".to_string())]);
		let eu = Partition::of(&[Value::Utf8("eu".to_string())]);
		cat.set_column_snapshot(
			ColumnSnapshotId(1),
			CommitVersion(1),
			Some(partitioned_series_snap(1, 7, 100, us)),
		);
		cat.set_column_snapshot(
			ColumnSnapshotId(2),
			CommitVersion(1),
			Some(partitioned_series_snap(2, 7, 100, eu)),
		);

		assert_eq!(cat.list_column_snapshots_for_series_at(SeriesId(7), CommitVersion(1)).len(), 2);
	}

	#[test]
	fn latest_table_snapshot_returns_max_version() {
		let cat = CatalogCache::new();
		cat.set_column_snapshot(ColumnSnapshotId(1), CommitVersion(1), Some(table_snap(1, 9, 5)));
		cat.set_column_snapshot(ColumnSnapshotId(2), CommitVersion(1), Some(table_snap(2, 9, 10)));
		let latest = cat
			.find_latest_column_snapshot_for_table_at(TableId(9), CommitVersion(1))
			.expect("should find");
		assert_eq!(latest.read_version(), CommitVersion(10));
	}

	#[test]
	fn delete_removes_secondary_indexes() {
		let cat = CatalogCache::new();
		cat.set_column_snapshot(ColumnSnapshotId(1), CommitVersion(1), Some(series_snap(1, 7, 100, 1)));
		assert!(cat.find_column_snapshot_for_series_bucket_at(SeriesId(7), None, 100, CommitVersion(1)).is_some());
		cat.set_column_snapshot(ColumnSnapshotId(1), CommitVersion(2), None);
		assert!(cat.find_column_snapshot_for_series_bucket_at(SeriesId(7), None, 100, CommitVersion(2)).is_none());
	}

	#[test]
	fn historical_query_sees_pre_delete_value() {
		// A delete is version-scoped: readers pinned before it must still see the row.
		let cat = CatalogCache::new();
		cat.set_column_snapshot(ColumnSnapshotId(1), CommitVersion(1), Some(series_snap(1, 7, 100, 1)));
		cat.set_column_snapshot(ColumnSnapshotId(1), CommitVersion(2), None);

		assert!(cat.find_column_snapshot_at(ColumnSnapshotId(1), CommitVersion(1)).is_some());
		assert!(cat.find_column_snapshot_at(ColumnSnapshotId(1), CommitVersion(2)).is_none());
	}

	#[test]
	fn version_below_insert_returns_none() {
		let cat = CatalogCache::new();
		cat.set_column_snapshot(ColumnSnapshotId(1), CommitVersion(10), Some(series_snap(1, 7, 0, 1)));

		assert!(cat.find_column_snapshot_at(ColumnSnapshotId(1), CommitVersion(5)).is_none());
		assert!(cat.find_column_snapshot_at(ColumnSnapshotId(1), CommitVersion(10)).is_some());
		assert!(cat.find_column_snapshot_at(ColumnSnapshotId(1), CommitVersion(20)).is_some());
	}

	#[test]
	fn list_for_series_isolates_per_series() {
		let cat = CatalogCache::new();
		cat.set_column_snapshot(ColumnSnapshotId(1), CommitVersion(1), Some(series_snap(1, 7, 0, 1)));
		cat.set_column_snapshot(ColumnSnapshotId(2), CommitVersion(1), Some(series_snap(2, 7, 100, 1)));
		cat.set_column_snapshot(ColumnSnapshotId(3), CommitVersion(1), Some(series_snap(3, 8, 0, 1)));

		let s7 = cat.list_column_snapshots_for_series_at(SeriesId(7), CommitVersion(1));
		let s8 = cat.list_column_snapshots_for_series_at(SeriesId(8), CommitVersion(1));
		assert_eq!(s7.len(), 2);
		assert_eq!(s8.len(), 1);
	}

	#[test]
	fn list_for_table_orders_by_commit_version_ascending() {
		let cat = CatalogCache::new();
		cat.set_column_snapshot(ColumnSnapshotId(1), CommitVersion(1), Some(table_snap(1, 9, 12)));
		cat.set_column_snapshot(ColumnSnapshotId(2), CommitVersion(1), Some(table_snap(2, 9, 5)));
		cat.set_column_snapshot(ColumnSnapshotId(3), CommitVersion(1), Some(table_snap(3, 9, 8)));

		let all = cat.list_column_snapshots_for_table_at(TableId(9), CommitVersion(1));
		let versions: Vec<CommitVersion> = all.iter().map(|s| s.read_version()).collect();
		assert_eq!(versions, vec![CommitVersion(5), CommitVersion(8), CommitVersion(12)]);
	}

	#[test]
	fn find_returns_latest_matching_version() {
		// The unversioned accessor must resolve to the newest version, not the first.
		let cat = CatalogCache::new();
		let mut v1 = series_snap(1, 7, 100, 1);
		v1.row_count = 10;
		let mut v2 = series_snap(1, 7, 100, 1);
		v2.row_count = 99;
		cat.set_column_snapshot(ColumnSnapshotId(1), CommitVersion(1), Some(v1));
		cat.set_column_snapshot(ColumnSnapshotId(1), CommitVersion(2), Some(v2));

		let latest = cat.find_column_snapshot(ColumnSnapshotId(1)).expect("present");
		assert_eq!(latest.row_count, 99);
	}
}
