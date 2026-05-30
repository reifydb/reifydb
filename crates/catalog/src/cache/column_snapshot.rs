// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		column_snapshot::{ColumnSnapshot, ColumnSnapshotSource},
		id::{ColumnSnapshotId, SeriesId, TableId},
	},
};

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
		bucket_start: u64,
		version: CommitVersion,
	) -> Option<ColumnSnapshot> {
		let entry = self.column_snapshots_for_series.get(&series_id)?;
		let buckets = entry.value();

		for (bs, snap_id) in buckets.iter() {
			if *bs == bucket_start
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
		for (_bs, snap_id) in buckets.iter() {
			if let Some(snap) = self.find_column_snapshot_at(*snap_id, version) {
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
				..
			} => {
				let mut existing = self
					.column_snapshots_for_series
					.get(series_id)
					.map(|e| e.value().clone())
					.unwrap_or_default();
				existing.insert((*bucket_start, snap.id));
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
				..
			} => {
				if let Some(entry) = self.column_snapshots_for_series.get(series_id) {
					let mut updated = entry.value().clone();
					updated.remove(&(*bucket_start, snap.id));
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

	use super::*;

	fn series_snap(id: u64, series_id: u64, bucket_start: u64, sealed_at: u64) -> ColumnSnapshot {
		ColumnSnapshot {
			id: ColumnSnapshotId(id),
			namespace: NamespaceId(1),
			source: ColumnSnapshotSource::SeriesBucket {
				series_id: SeriesId(series_id),
				bucket_start,
				bucket_width: 100,
				sequence_counter: 0,
				sealed_at_commit_version: CommitVersion(sealed_at),
			},
			row_count: 0,
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
			.find_column_snapshot_for_series_bucket_at(SeriesId(7), 100, CommitVersion(1))
			.expect("should find");
		assert_eq!(found.id, ColumnSnapshotId(1));

		assert!(cat.find_column_snapshot_for_series_bucket_at(SeriesId(7), 999, CommitVersion(1)).is_none());
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
		assert!(cat.find_column_snapshot_for_series_bucket_at(SeriesId(7), 100, CommitVersion(1)).is_some());
		cat.set_column_snapshot(ColumnSnapshotId(1), CommitVersion(2), None);
		assert!(cat.find_column_snapshot_for_series_bucket_at(SeriesId(7), 100, CommitVersion(2)).is_none());
	}

	#[test]
	fn historical_query_sees_pre_delete_value() {
		// MultiVersionContainer keeps prior versions readable. Inserting at v=1
		// then deleting at v=2 means a query at v=1 should still see the row.
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

		// Snapshot was inserted at v=10; querying at v=5 must return None.
		assert!(cat.find_column_snapshot_at(ColumnSnapshotId(1), CommitVersion(5)).is_none());
		assert!(cat.find_column_snapshot_at(ColumnSnapshotId(1), CommitVersion(10)).is_some());
		// And later versions inherit the v=10 value.
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
		// `find_column_snapshot` (no `_at`) should return the latest stored
		// representation regardless of version.
		let cat = CatalogCache::new();
		let mut v1 = series_snap(1, 7, 100, 1);
		// Mutate row_count between versions to prove latest wins.
		v1.row_count = 10;
		let mut v2 = series_snap(1, 7, 100, 1);
		v2.row_count = 99;
		cat.set_column_snapshot(ColumnSnapshotId(1), CommitVersion(1), Some(v1));
		cat.set_column_snapshot(ColumnSnapshotId(1), CommitVersion(2), Some(v2));

		let latest = cat.find_column_snapshot(ColumnSnapshotId(1)).expect("present");
		assert_eq!(latest.row_count, 99);
	}
}
