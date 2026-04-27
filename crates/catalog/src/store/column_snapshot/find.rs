// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	encoded::row::EncodedRow,
	interface::catalog::{
		column_snapshot::{ColumnSnapshot, ColumnSnapshotKind, ColumnSnapshotSource},
		id::{ColumnSnapshotId, NamespaceId, SeriesId, TableId},
	},
	key::column_snapshot::{ColumnSnapshotKey, SeriesColumnSnapshotKey, TableColumnSnapshotKey},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use crate::{
	CatalogStore, Result,
	store::column_snapshot::shape::{column_snapshot, column_snapshot_link},
};

impl CatalogStore {
	pub(crate) fn find_column_snapshot(
		rx: &mut Transaction<'_>,
		id: ColumnSnapshotId,
	) -> Result<Option<ColumnSnapshot>> {
		let Some(multi) = rx.get(&ColumnSnapshotKey::encoded(id))? else {
			return Ok(None);
		};
		Ok(Some(decode_column_snapshot(&multi.row)))
	}

	pub(crate) fn find_column_snapshot_for_series_bucket(
		rx: &mut Transaction<'_>,
		series_id: SeriesId,
		bucket_start: u64,
	) -> Result<Option<ColumnSnapshot>> {
		for id in collect_series_snapshot_ids(rx, series_id)? {
			if let Some(snap) = Self::find_column_snapshot(rx, id)?
				&& let ColumnSnapshotSource::SeriesBucket {
					bucket_start: bs,
					..
				} = snap.source && bs == bucket_start
			{
				return Ok(Some(snap));
			}
		}
		Ok(None)
	}

	pub(crate) fn find_latest_column_snapshot_for_table(
		rx: &mut Transaction<'_>,
		table_id: TableId,
	) -> Result<Option<ColumnSnapshot>> {
		let mut latest: Option<ColumnSnapshot> = None;
		for id in collect_table_snapshot_ids(rx, table_id)? {
			if let Some(snap) = Self::find_column_snapshot(rx, id)? {
				match &latest {
					Some(prev) if snap.read_version() <= prev.read_version() => {}
					_ => latest = Some(snap),
				}
			}
		}
		Ok(latest)
	}
}

pub(crate) fn collect_series_snapshot_ids(
	rx: &mut Transaction<'_>,
	series_id: SeriesId,
) -> Result<Vec<ColumnSnapshotId>> {
	let mut ids = Vec::new();
	let mut stream = rx.range(SeriesColumnSnapshotKey::full_scan(series_id), RangeScope::All, 1024)?;
	for entry in stream.by_ref() {
		let multi = entry?;
		ids.push(ColumnSnapshotId(column_snapshot_link::SHAPE.get_u64(&multi.row, column_snapshot_link::ID)));
	}
	drop(stream);
	Ok(ids)
}

pub(crate) fn collect_table_snapshot_ids(rx: &mut Transaction<'_>, table_id: TableId) -> Result<Vec<ColumnSnapshotId>> {
	let mut ids = Vec::new();
	let mut stream = rx.range(TableColumnSnapshotKey::full_scan(table_id), RangeScope::All, 1024)?;
	for entry in stream.by_ref() {
		let multi = entry?;
		ids.push(ColumnSnapshotId(column_snapshot_link::SHAPE.get_u64(&multi.row, column_snapshot_link::ID)));
	}
	drop(stream);
	Ok(ids)
}

pub(crate) fn decode_column_snapshot(row: &EncodedRow) -> ColumnSnapshot {
	let id = ColumnSnapshotId(column_snapshot::SHAPE.get_u64(row, column_snapshot::ID));
	let namespace = NamespaceId(column_snapshot::SHAPE.get_u64(row, column_snapshot::NAMESPACE));
	let kind_byte = column_snapshot::SHAPE.get_u8(row, column_snapshot::KIND);
	let kind = ColumnSnapshotKind::try_from(kind_byte).expect("invalid stored ColumnSnapshotKind");
	let source_id = column_snapshot::SHAPE.get_u64(row, column_snapshot::SOURCE_ID);
	let bucket_start = column_snapshot::SHAPE.get_u64(row, column_snapshot::BUCKET_START);
	let bucket_width = column_snapshot::SHAPE.get_u64(row, column_snapshot::BUCKET_WIDTH);
	let sequence_counter = column_snapshot::SHAPE.get_u64(row, column_snapshot::SEQUENCE_COUNTER);
	let read_version = CommitVersion(column_snapshot::SHAPE.get_u64(row, column_snapshot::READ_VERSION));
	let row_count = column_snapshot::SHAPE.get_u64(row, column_snapshot::ROW_COUNT);

	let source = match kind {
		ColumnSnapshotKind::Table => ColumnSnapshotSource::Table {
			table_id: TableId(source_id),
			commit_version: read_version,
		},
		ColumnSnapshotKind::SeriesBucket => ColumnSnapshotSource::SeriesBucket {
			series_id: SeriesId(source_id),
			bucket_start,
			bucket_width,
			sequence_counter,
			sealed_at_commit_version: read_version,
		},
	};

	ColumnSnapshot {
		id,
		namespace,
		source,
		row_count,
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		common::CommitVersion,
		interface::catalog::{
			column_snapshot::ColumnSnapshotSource,
			id::{ColumnSnapshotId, NamespaceId, SeriesId, TableId},
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

	#[test]
	fn test_find_column_snapshot_exists() {
		let mut txn = create_test_admin_transaction();
		let created = CatalogStore::create_column_snapshot(&mut txn, table_to_create(101, 7)).unwrap();

		let found = CatalogStore::find_column_snapshot(&mut Transaction::Admin(&mut txn), created.id)
			.unwrap()
			.expect("snapshot should exist");

		assert_eq!(found.id, created.id);
		assert_eq!(found.namespace, NamespaceId(1));
		assert_eq!(found.read_version(), CommitVersion(7));
	}

	#[test]
	fn test_find_column_snapshot_not_exists() {
		let mut txn = create_test_admin_transaction();
		let result = CatalogStore::find_column_snapshot(
			&mut Transaction::Admin(&mut txn),
			ColumnSnapshotId(999_999),
		)
		.unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_find_column_snapshot_for_series_bucket_exists() {
		let mut txn = create_test_admin_transaction();
		let created = CatalogStore::create_column_snapshot(&mut txn, series_to_create(202, 1000, 11)).unwrap();

		let found = CatalogStore::find_column_snapshot_for_series_bucket(
			&mut Transaction::Admin(&mut txn),
			SeriesId(202),
			1000,
		)
		.unwrap()
		.expect("snapshot for bucket should exist");

		assert_eq!(found.id, created.id);
	}

	#[test]
	fn test_find_column_snapshot_for_series_bucket_wrong_bucket_returns_none() {
		let mut txn = create_test_admin_transaction();
		CatalogStore::create_column_snapshot(&mut txn, series_to_create(202, 1000, 11)).unwrap();

		let result = CatalogStore::find_column_snapshot_for_series_bucket(
			&mut Transaction::Admin(&mut txn),
			SeriesId(202),
			500,
		)
		.unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_find_column_snapshot_for_series_bucket_wrong_series_returns_none() {
		let mut txn = create_test_admin_transaction();
		CatalogStore::create_column_snapshot(&mut txn, series_to_create(202, 1000, 11)).unwrap();

		let result = CatalogStore::find_column_snapshot_for_series_bucket(
			&mut Transaction::Admin(&mut txn),
			SeriesId(999),
			1000,
		)
		.unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_find_latest_column_snapshot_for_table_picks_max_read_version() {
		let mut txn = create_test_admin_transaction();
		let _older = CatalogStore::create_column_snapshot(&mut txn, table_to_create(50, 5)).unwrap();
		let newer = CatalogStore::create_column_snapshot(&mut txn, table_to_create(50, 12)).unwrap();
		let _middle = CatalogStore::create_column_snapshot(&mut txn, table_to_create(50, 8)).unwrap();

		let latest = CatalogStore::find_latest_column_snapshot_for_table(
			&mut Transaction::Admin(&mut txn),
			TableId(50),
		)
		.unwrap()
		.expect("should find latest");

		assert_eq!(latest.id, newer.id);
		assert_eq!(latest.read_version(), CommitVersion(12));
	}

	#[test]
	fn test_find_latest_column_snapshot_for_table_not_exists() {
		let mut txn = create_test_admin_transaction();
		let result = CatalogStore::find_latest_column_snapshot_for_table(
			&mut Transaction::Admin(&mut txn),
			TableId(123),
		)
		.unwrap();
		assert!(result.is_none());
	}
}
