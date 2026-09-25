// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::{catalog::EncodedCatalogRow, pod::EncodedPodRow};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		column_snapshot::{ColumnSnapshot, ColumnSnapshotKind, ColumnSnapshotSource},
		id::{ColumnSnapshotId, NamespaceId, SeriesId, TableId},
	},
	key::column::{ColumnSnapshotKey, SeriesColumnSnapshotKey, TableColumnSnapshotKey},
	return_internal_error,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::value::partition::Partition;

use crate::{
	CatalogStore, Result,
	store::column_snapshot::shape::{column_snapshot, deserialize_partition_values, deserialize_stats},
};

fn decode_snapshot_link(row: &EncodedPodRow) -> Result<u64> {
	let Ok(bytes) = <[u8; 8]>::try_from(row.body()) else {
		return_internal_error!(
			"Column snapshot link is {} bytes wide, expected 8. This indicates a corrupt link row.",
			row.len()
		)
	};
	Ok(u64::from_be_bytes(bytes))
}

impl CatalogStore {
	pub(crate) fn find_column_snapshot(
		rx: &mut Transaction<'_>,
		id: ColumnSnapshotId,
	) -> Result<Option<ColumnSnapshot>> {
		let Some(multi) = rx.get(&ColumnSnapshotKey::new(id))? else {
			return Ok(None);
		};
		Ok(Some(decode_column_snapshot(EncodedCatalogRow::view(&multi.bytes))))
	}

	pub(crate) fn find_column_snapshot_for_series_bucket(
		rx: &mut Transaction<'_>,
		series_id: SeriesId,
		partition: Option<Partition>,
		bucket_start: u64,
	) -> Result<Option<ColumnSnapshot>> {
		for id in collect_series_partition_snapshot_ids(rx, series_id, partition.unwrap_or_default())? {
			if let Some(snap) = Self::find_column_snapshot(rx, id)?
				&& let ColumnSnapshotSource::SeriesBucket {
					bucket_start: bs,
					partition: p,
					..
				} = snap.source
				&& bs == bucket_start
				&& p == partition
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
		ids.push(ColumnSnapshotId(decode_snapshot_link(EncodedPodRow::view(&multi.bytes))?));
	}
	drop(stream);
	Ok(ids)
}

pub(crate) fn collect_series_partition_snapshot_ids(
	rx: &mut Transaction<'_>,
	series_id: SeriesId,
	partition: Partition,
) -> Result<Vec<ColumnSnapshotId>> {
	let mut ids = Vec::new();
	let mut stream =
		rx.range(SeriesColumnSnapshotKey::partition_scan(series_id, partition), RangeScope::All, 1024)?;
	for entry in stream.by_ref() {
		let multi = entry?;
		ids.push(ColumnSnapshotId(decode_snapshot_link(EncodedPodRow::view(&multi.bytes))?));
	}
	drop(stream);
	Ok(ids)
}

pub(crate) fn collect_table_snapshot_ids(rx: &mut Transaction<'_>, table_id: TableId) -> Result<Vec<ColumnSnapshotId>> {
	let mut ids = Vec::new();
	let mut stream = rx.range(TableColumnSnapshotKey::full_scan(table_id), RangeScope::All, 1024)?;
	for entry in stream.by_ref() {
		let multi = entry?;
		ids.push(ColumnSnapshotId(decode_snapshot_link(EncodedPodRow::view(&multi.bytes))?));
	}
	drop(stream);
	Ok(ids)
}

pub(crate) fn decode_column_snapshot(bytes: &EncodedCatalogRow) -> ColumnSnapshot {
	let id = ColumnSnapshotId(column_snapshot::get_id(bytes));
	let namespace = NamespaceId(column_snapshot::get_namespace(bytes));
	let kind_byte = column_snapshot::get_kind(bytes);
	let kind = ColumnSnapshotKind::try_from(kind_byte).expect("invalid stored ColumnSnapshotKind");
	let source_id = column_snapshot::get_source_id(bytes);
	let bucket_start = column_snapshot::get_bucket_start(bytes);
	let bucket_width = column_snapshot::get_bucket_width(bytes);
	let sequence_counter = column_snapshot::get_sequence_counter(bytes);
	let read_version = CommitVersion(column_snapshot::get_read_version(bytes));
	let row_count = column_snapshot::get_row_count(bytes);
	let partition = column_snapshot::try_get_partition_hi(bytes)
		.zip(column_snapshot::try_get_partition_lo(bytes))
		.map(|(hi, lo)| Partition(((hi as u128) << 64) | lo as u128));

	let source = match kind {
		ColumnSnapshotKind::Table => ColumnSnapshotSource::Table {
			table_id: TableId(source_id),
			commit_version: read_version,
		},
		ColumnSnapshotKind::SeriesBucket => ColumnSnapshotSource::SeriesBucket {
			series_id: SeriesId(source_id),
			bucket_start,
			bucket_width,
			partition,
			sequence_counter,
			sealed_at_commit_version: read_version,
		},
	};

	ColumnSnapshot {
		id,
		namespace,
		source,
		row_count,
		partition_values: deserialize_partition_values(&column_snapshot::get_partition_values(bytes)),
		stats: deserialize_stats(&column_snapshot::get_stats(bytes)),
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
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::value::{Value, partition::Partition};

	use crate::{CatalogStore, store::column_snapshot::create::ColumnSnapshotToCreate};

	fn partitioned_series_to_create(
		series: u64,
		bucket_start: u64,
		partition: Partition,
		region: &str,
	) -> ColumnSnapshotToCreate {
		ColumnSnapshotToCreate {
			namespace: NamespaceId(1),
			source: ColumnSnapshotSource::SeriesBucket {
				series_id: SeriesId(series),
				bucket_start,
				bucket_width: 100,
				partition: Some(partition),
				sequence_counter: 0,
				sealed_at_commit_version: CommitVersion(1),
			},
			row_count: 0,
			partition_values: vec![Value::Utf8(region.to_string())],
			stats: Vec::new(),
		}
	}

	fn series_to_create(series: u64, bucket_start: u64, sealed_at: u64) -> ColumnSnapshotToCreate {
		ColumnSnapshotToCreate {
			namespace: NamespaceId(1),
			source: ColumnSnapshotSource::SeriesBucket {
				series_id: SeriesId(series),
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

	fn table_to_create(table: u64, commit_version: u64) -> ColumnSnapshotToCreate {
		ColumnSnapshotToCreate {
			namespace: NamespaceId(1),
			source: ColumnSnapshotSource::Table {
				table_id: TableId(table),
				commit_version: CommitVersion(commit_version),
			},
			row_count: 0,
			partition_values: Vec::new(),
			stats: Vec::new(),
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
			None,
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
			None,
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
			None,
			1000,
		)
		.unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_two_partitions_of_one_bucket_resolve_to_distinct_snapshots() {
		// without the partition in the lookup both partitions of a bucket resolve to the
		// same snapshot, so materializing the second overwrites the first block
		let mut txn = create_test_admin_transaction();
		let us = Partition::of(&[Value::Utf8("us".to_string())]);
		let eu = Partition::of(&[Value::Utf8("eu".to_string())]);
		let us_snap = CatalogStore::create_column_snapshot(
			&mut txn,
			partitioned_series_to_create(202, 1000, us, "us"),
		)
		.unwrap();
		let eu_snap = CatalogStore::create_column_snapshot(
			&mut txn,
			partitioned_series_to_create(202, 1000, eu, "eu"),
		)
		.unwrap();
		assert_ne!(us_snap.id, eu_snap.id, "each partition must get its own snapshot");

		let found_us = CatalogStore::find_column_snapshot_for_series_bucket(
			&mut Transaction::Admin(&mut txn),
			SeriesId(202),
			Some(us),
			1000,
		)
		.unwrap()
		.expect("the us partition of the bucket must be found");
		let found_eu = CatalogStore::find_column_snapshot_for_series_bucket(
			&mut Transaction::Admin(&mut txn),
			SeriesId(202),
			Some(eu),
			1000,
		)
		.unwrap()
		.expect("the eu partition of the bucket must be found");

		assert_eq!(found_us.id, us_snap.id);
		assert_eq!(found_eu.id, eu_snap.id);
	}

	#[test]
	fn test_unpartitioned_lookup_does_not_match_a_partitioned_bucket() {
		// a none partition and a hashed one are different buckets, so neither may stand in
		// for the other and hand the reader the wrong block
		let mut txn = create_test_admin_transaction();
		let us = Partition::of(&[Value::Utf8("us".to_string())]);
		CatalogStore::create_column_snapshot(&mut txn, partitioned_series_to_create(202, 1000, us, "us"))
			.unwrap();

		let result = CatalogStore::find_column_snapshot_for_series_bucket(
			&mut Transaction::Admin(&mut txn),
			SeriesId(202),
			None,
			1000,
		)
		.unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_partition_and_partition_values_and_stats_survive_a_round_trip() {
		// these three ride the row as two optional halves and two blobs; a decode that
		// loses any of them leaves pruning with nothing to prune on
		let mut txn = create_test_admin_transaction();
		let us = Partition::of(&[Value::Utf8("us".to_string())]);
		let created = CatalogStore::create_column_snapshot(
			&mut txn,
			partitioned_series_to_create(202, 1000, us, "us"),
		)
		.unwrap();

		let found = CatalogStore::find_column_snapshot(&mut Transaction::Admin(&mut txn), created.id)
			.unwrap()
			.expect("snapshot should exist");

		assert_eq!(found.partition_values, vec![Value::Utf8("us".to_string())]);
		match found.source {
			ColumnSnapshotSource::SeriesBucket {
				partition,
				..
			} => assert_eq!(partition, Some(us)),
			other => panic!("expected SeriesBucket source, got {other:?}"),
		}
	}

	#[test]
	fn test_unpartitioned_snapshot_decodes_partition_as_none() {
		// a zero hash and an absent partition must stay distinguishable, so the row stores
		// the halves as none rather than as two zeroes
		let mut txn = create_test_admin_transaction();
		let created = CatalogStore::create_column_snapshot(&mut txn, series_to_create(202, 1000, 11)).unwrap();

		let found = CatalogStore::find_column_snapshot(&mut Transaction::Admin(&mut txn), created.id)
			.unwrap()
			.expect("snapshot should exist");

		match found.source {
			ColumnSnapshotSource::SeriesBucket {
				partition,
				..
			} => assert_eq!(partition, None),
			other => panic!("expected SeriesBucket source, got {other:?}"),
		}
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
