// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		column_snapshot::{ColumnSnapshot, ColumnSnapshotKind, ColumnSnapshotSource},
		id::{ColumnSnapshotId, NamespaceId},
	},
	key::column_snapshot::{ColumnSnapshotKey, SeriesColumnSnapshotKey, TableColumnSnapshotKey},
};
use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::{
	CatalogStore, Result,
	store::{
		column_snapshot::shape::{column_snapshot, column_snapshot_link},
		sequence::system::SystemSequence,
	},
};

#[derive(Debug, Clone)]
pub struct ColumnSnapshotToCreate {
	pub namespace: NamespaceId,
	pub source: ColumnSnapshotSource,
	pub row_count: u64,
}

impl CatalogStore {
	pub(crate) fn create_column_snapshot(
		txn: &mut AdminTransaction,
		to_create: ColumnSnapshotToCreate,
	) -> Result<ColumnSnapshot> {
		let id = SystemSequence::next_column_snapshot_id(txn)?;
		Self::store_column_snapshot(txn, id, &to_create)?;
		Self::link_column_snapshot(txn, id, &to_create.source)?;

		Ok(ColumnSnapshot {
			id,
			namespace: to_create.namespace,
			source: to_create.source,
			row_count: to_create.row_count,
		})
	}

	pub(crate) fn store_column_snapshot(
		txn: &mut AdminTransaction,
		id: ColumnSnapshotId,
		to_create: &ColumnSnapshotToCreate,
	) -> Result<()> {
		let mut row = column_snapshot::SHAPE.allocate();
		column_snapshot::SHAPE.set_u64(&mut row, column_snapshot::ID, id);
		column_snapshot::SHAPE.set_u64(&mut row, column_snapshot::NAMESPACE, to_create.namespace);
		column_snapshot::SHAPE.set_u8(&mut row, column_snapshot::KIND, to_create.source.kind() as u8);

		match &to_create.source {
			ColumnSnapshotSource::Table {
				table_id,
				commit_version,
			} => {
				column_snapshot::SHAPE.set_u64(&mut row, column_snapshot::SOURCE_ID, *table_id);
				column_snapshot::SHAPE.set_u64(&mut row, column_snapshot::BUCKET_START, 0u64);
				column_snapshot::SHAPE.set_u64(&mut row, column_snapshot::BUCKET_WIDTH, 0u64);
				column_snapshot::SHAPE.set_u64(&mut row, column_snapshot::SEQUENCE_COUNTER, 0u64);
				column_snapshot::SHAPE.set_u64(
					&mut row,
					column_snapshot::READ_VERSION,
					commit_version.0,
				);
			}
			ColumnSnapshotSource::SeriesBucket {
				series_id,
				bucket_start,
				bucket_width,
				sequence_counter,
				sealed_at_commit_version,
			} => {
				column_snapshot::SHAPE.set_u64(&mut row, column_snapshot::SOURCE_ID, *series_id);
				column_snapshot::SHAPE.set_u64(&mut row, column_snapshot::BUCKET_START, *bucket_start);
				column_snapshot::SHAPE.set_u64(&mut row, column_snapshot::BUCKET_WIDTH, *bucket_width);
				column_snapshot::SHAPE.set_u64(
					&mut row,
					column_snapshot::SEQUENCE_COUNTER,
					*sequence_counter,
				);
				column_snapshot::SHAPE.set_u64(
					&mut row,
					column_snapshot::READ_VERSION,
					sealed_at_commit_version.0,
				);
			}
		}

		column_snapshot::SHAPE.set_u64(&mut row, column_snapshot::ROW_COUNT, to_create.row_count);

		txn.set(&ColumnSnapshotKey::encoded(id), row)?;
		Ok(())
	}

	pub(crate) fn link_column_snapshot(
		txn: &mut AdminTransaction,
		id: ColumnSnapshotId,
		source: &ColumnSnapshotSource,
	) -> Result<()> {
		let mut row = column_snapshot_link::SHAPE.allocate();
		column_snapshot_link::SHAPE.set_u64(&mut row, column_snapshot_link::ID, id);

		match source {
			ColumnSnapshotSource::Table {
				table_id,
				..
			} => {
				txn.set(&TableColumnSnapshotKey::encoded(*table_id, id), row)?;
			}
			ColumnSnapshotSource::SeriesBucket {
				series_id,
				..
			} => {
				txn.set(&SeriesColumnSnapshotKey::encoded(*series_id, id), row)?;
			}
		}

		let _ = ColumnSnapshotKind::Table;

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		common::CommitVersion,
		interface::catalog::{
			column_snapshot::ColumnSnapshotSource,
			id::{NamespaceId, SeriesId, TableId},
		},
	};
	use reifydb_engine::test_harness::create_test_admin_transaction;

	use crate::{CatalogStore, store::column_snapshot::create::ColumnSnapshotToCreate};

	#[test]
	fn test_create_table_column_snapshot() {
		let mut txn = create_test_admin_transaction();

		let snap = CatalogStore::create_column_snapshot(
			&mut txn,
			ColumnSnapshotToCreate {
				namespace: NamespaceId(1),
				source: ColumnSnapshotSource::Table {
					table_id: TableId(101),
					commit_version: CommitVersion(7),
				},
				row_count: 42,
			},
		)
		.unwrap();

		assert!(snap.id.0 > 0);
		assert_eq!(snap.namespace, NamespaceId(1));
		assert_eq!(snap.row_count, 42);
		assert_eq!(snap.read_version(), CommitVersion(7));
	}

	#[test]
	fn test_create_series_bucket_column_snapshot() {
		let mut txn = create_test_admin_transaction();

		let snap = CatalogStore::create_column_snapshot(
			&mut txn,
			ColumnSnapshotToCreate {
				namespace: NamespaceId(1),
				source: ColumnSnapshotSource::SeriesBucket {
					series_id: SeriesId(202),
					bucket_start: 1000,
					bucket_width: 100,
					sequence_counter: 5,
					sealed_at_commit_version: CommitVersion(11),
				},
				row_count: 50,
			},
		)
		.unwrap();

		assert!(snap.id.0 > 0);
		assert_eq!(snap.read_version(), CommitVersion(11));
		assert_eq!(snap.source.series_bucket_range(), Some((1000, 1100)));
	}
}
