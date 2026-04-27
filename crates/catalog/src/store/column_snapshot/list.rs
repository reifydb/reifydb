// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	column_snapshot::{ColumnSnapshot, ColumnSnapshotSource},
	id::{SeriesId, TableId},
};
use reifydb_transaction::transaction::Transaction;

use crate::{
	CatalogStore, Result,
	store::column_snapshot::find::{collect_series_snapshot_ids, collect_table_snapshot_ids},
};

impl CatalogStore {
	pub(crate) fn list_column_snapshots_for_series(
		rx: &mut Transaction<'_>,
		series_id: SeriesId,
	) -> Result<Vec<ColumnSnapshot>> {
		let ids = collect_series_snapshot_ids(rx, series_id)?;
		let mut out = Vec::with_capacity(ids.len());
		for id in ids {
			if let Some(snap) = Self::find_column_snapshot(rx, id)? {
				out.push(snap);
			}
		}

		out.sort_by_key(|s| match s.source {
			ColumnSnapshotSource::SeriesBucket {
				bucket_start,
				..
			} => bucket_start,
			_ => 0,
		});
		Ok(out)
	}

	pub(crate) fn list_column_snapshots_for_table(
		rx: &mut Transaction<'_>,
		table_id: TableId,
	) -> Result<Vec<ColumnSnapshot>> {
		let ids = collect_table_snapshot_ids(rx, table_id)?;
		let mut out = Vec::with_capacity(ids.len());
		for id in ids {
			if let Some(snap) = Self::find_column_snapshot(rx, id)? {
				out.push(snap);
			}
		}

		out.sort_by_key(|s| s.read_version());
		Ok(out)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		common::CommitVersion,
		interface::catalog::{
			column_snapshot::{ColumnSnapshot, ColumnSnapshotSource},
			id::{NamespaceId, SeriesId, TableId},
		},
	};
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::{CatalogStore, store::column_snapshot::create::ColumnSnapshotToCreate};

	fn series_to_create(series: u64, bucket_start: u64, sealed_at: u64) -> ColumnSnapshotToCreate {
		ColumnSnapshotToCreate {
			namespace: NamespaceId(1),
			source: ColumnSnapshotSource::SeriesBucket {
				series_id: SeriesId(series),
				bucket_start,
				bucket_width: 100,
				sequence_counter: 0,
				sealed_at_commit_version: CommitVersion(sealed_at),
			},
			row_count: 0,
		}
	}

	fn table_to_create(table: u64, commit_version: u64) -> ColumnSnapshotToCreate {
		ColumnSnapshotToCreate {
			namespace: NamespaceId(1),
			source: ColumnSnapshotSource::Table {
				table_id: TableId(table),
				commit_version: CommitVersion(commit_version),
			},
			row_count: 0,
		}
	}

	fn series_bucket_starts(snaps: &[ColumnSnapshot]) -> Vec<u64> {
		snaps.iter()
			.map(|s| match s.source {
				ColumnSnapshotSource::SeriesBucket {
					bucket_start,
					..
				} => bucket_start,
				_ => panic!("expected series-bucket source"),
			})
			.collect()
	}

	#[test]
	fn test_list_column_snapshots_for_series_empty() {
		let mut txn = create_test_admin_transaction();
		let result =
			CatalogStore::list_column_snapshots_for_series(&mut Transaction::Admin(&mut txn), SeriesId(1))
				.unwrap();
		assert!(result.is_empty());
	}

	#[test]
	fn test_list_column_snapshots_for_series_orders_by_bucket_start() {
		let mut txn = create_test_admin_transaction();
		// Insert in non-monotonic bucket order to prove the sort.
		CatalogStore::create_column_snapshot(&mut txn, series_to_create(7, 200, 1)).unwrap();
		CatalogStore::create_column_snapshot(&mut txn, series_to_create(7, 0, 1)).unwrap();
		CatalogStore::create_column_snapshot(&mut txn, series_to_create(7, 100, 1)).unwrap();

		let result =
			CatalogStore::list_column_snapshots_for_series(&mut Transaction::Admin(&mut txn), SeriesId(7))
				.unwrap();

		assert_eq!(result.len(), 3);
		assert_eq!(series_bucket_starts(&result), vec![0u64, 100, 200]);
	}

	#[test]
	fn test_list_column_snapshots_for_series_isolates_by_series() {
		let mut txn = create_test_admin_transaction();
		CatalogStore::create_column_snapshot(&mut txn, series_to_create(1, 0, 1)).unwrap();
		CatalogStore::create_column_snapshot(&mut txn, series_to_create(2, 0, 1)).unwrap();
		CatalogStore::create_column_snapshot(&mut txn, series_to_create(2, 100, 1)).unwrap();

		let s1 = CatalogStore::list_column_snapshots_for_series(&mut Transaction::Admin(&mut txn), SeriesId(1))
			.unwrap();
		let s2 = CatalogStore::list_column_snapshots_for_series(&mut Transaction::Admin(&mut txn), SeriesId(2))
			.unwrap();
		assert_eq!(s1.len(), 1);
		assert_eq!(s2.len(), 2);
	}

	#[test]
	fn test_list_column_snapshots_for_table_orders_by_read_version() {
		let mut txn = create_test_admin_transaction();
		CatalogStore::create_column_snapshot(&mut txn, table_to_create(50, 12)).unwrap();
		CatalogStore::create_column_snapshot(&mut txn, table_to_create(50, 5)).unwrap();
		CatalogStore::create_column_snapshot(&mut txn, table_to_create(50, 8)).unwrap();

		let result =
			CatalogStore::list_column_snapshots_for_table(&mut Transaction::Admin(&mut txn), TableId(50))
				.unwrap();

		assert_eq!(result.len(), 3);
		let versions: Vec<CommitVersion> = result.iter().map(|s| s.read_version()).collect();
		assert_eq!(versions, vec![CommitVersion(5), CommitVersion(8), CommitVersion(12)]);
	}

	#[test]
	fn test_list_column_snapshots_for_table_empty() {
		let mut txn = create_test_admin_transaction();
		let result =
			CatalogStore::list_column_snapshots_for_table(&mut Transaction::Admin(&mut txn), TableId(99))
				.unwrap();
		assert!(result.is_empty());
	}
}
