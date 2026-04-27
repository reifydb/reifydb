// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		column_snapshot::{ColumnSnapshot, ColumnSnapshotSource},
		id::ColumnSnapshotId,
	},
	key::column_snapshot::ColumnSnapshotKey,
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{CatalogStore, Result, store::column_snapshot::shape::column_snapshot};

#[derive(Debug, Clone)]
pub struct ColumnSnapshotToUpdate {
	pub sequence_counter: u64,
	pub read_version: CommitVersion,
	pub row_count: u64,
}

impl CatalogStore {
	pub(crate) fn update_column_snapshot(
		txn: &mut AdminTransaction,
		id: ColumnSnapshotId,
		patch: ColumnSnapshotToUpdate,
	) -> Result<ColumnSnapshot> {
		let existing = Self::get_column_snapshot(&mut Transaction::Admin(&mut *txn), id)?;

		let mut row = column_snapshot::SHAPE.allocate();
		column_snapshot::SHAPE.set_u64(&mut row, column_snapshot::ID, existing.id);
		column_snapshot::SHAPE.set_u64(&mut row, column_snapshot::NAMESPACE, existing.namespace);
		column_snapshot::SHAPE.set_u8(&mut row, column_snapshot::KIND, existing.source.kind() as u8);

		let updated_source = match existing.source {
			ColumnSnapshotSource::Table {
				table_id,
				..
			} => ColumnSnapshotSource::Table {
				table_id,
				commit_version: patch.read_version,
			},
			ColumnSnapshotSource::SeriesBucket {
				series_id,
				bucket_start,
				bucket_width,
				..
			} => ColumnSnapshotSource::SeriesBucket {
				series_id,
				bucket_start,
				bucket_width,
				sequence_counter: patch.sequence_counter,
				sealed_at_commit_version: patch.read_version,
			},
		};

		match &updated_source {
			ColumnSnapshotSource::Table {
				table_id,
				..
			} => {
				column_snapshot::SHAPE.set_u64(&mut row, column_snapshot::SOURCE_ID, *table_id);
				column_snapshot::SHAPE.set_u64(&mut row, column_snapshot::BUCKET_START, 0u64);
				column_snapshot::SHAPE.set_u64(&mut row, column_snapshot::BUCKET_WIDTH, 0u64);
				column_snapshot::SHAPE.set_u64(&mut row, column_snapshot::SEQUENCE_COUNTER, 0u64);
			}
			ColumnSnapshotSource::SeriesBucket {
				series_id,
				bucket_start,
				bucket_width,
				sequence_counter,
				..
			} => {
				column_snapshot::SHAPE.set_u64(&mut row, column_snapshot::SOURCE_ID, *series_id);
				column_snapshot::SHAPE.set_u64(&mut row, column_snapshot::BUCKET_START, *bucket_start);
				column_snapshot::SHAPE.set_u64(&mut row, column_snapshot::BUCKET_WIDTH, *bucket_width);
				column_snapshot::SHAPE.set_u64(
					&mut row,
					column_snapshot::SEQUENCE_COUNTER,
					*sequence_counter,
				);
			}
		}

		column_snapshot::SHAPE.set_u64(&mut row, column_snapshot::READ_VERSION, patch.read_version.0);
		column_snapshot::SHAPE.set_u64(&mut row, column_snapshot::ROW_COUNT, patch.row_count);

		txn.set(&ColumnSnapshotKey::encoded(existing.id), row)?;

		Ok(ColumnSnapshot {
			id: existing.id,
			namespace: existing.namespace,
			source: updated_source,
			row_count: patch.row_count,
		})
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

	use crate::{
		CatalogStore,
		store::column_snapshot::{create::ColumnSnapshotToCreate, update::ColumnSnapshotToUpdate},
	};

	#[test]
	fn test_update_series_bucket_preserves_id_and_immutable_fields() {
		let mut txn = create_test_admin_transaction();
		let created = CatalogStore::create_column_snapshot(
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

		let updated = CatalogStore::update_column_snapshot(
			&mut txn,
			created.id,
			ColumnSnapshotToUpdate {
				sequence_counter: 17,
				read_version: CommitVersion(42),
				row_count: 99,
			},
		)
		.unwrap();

		assert_eq!(updated.id, created.id, "id must be preserved");
		assert_eq!(updated.namespace, NamespaceId(1));
		assert_eq!(updated.row_count, 99);
		assert_eq!(updated.read_version(), CommitVersion(42));

		match updated.source {
			ColumnSnapshotSource::SeriesBucket {
				series_id,
				bucket_start,
				bucket_width,
				sequence_counter,
				sealed_at_commit_version,
			} => {
				assert_eq!(series_id, SeriesId(202));
				assert_eq!(bucket_start, 1000, "bucket_start is immutable");
				assert_eq!(bucket_width, 100, "bucket_width is immutable");
				assert_eq!(sequence_counter, 17, "sequence_counter must advance");
				assert_eq!(sealed_at_commit_version, CommitVersion(42));
			}
			other => panic!("expected SeriesBucket source, got {other:?}"),
		}

		// Round-trip: a re-find returns the updated values.
		let reread = CatalogStore::find_column_snapshot(&mut Transaction::Admin(&mut txn), created.id)
			.unwrap()
			.expect("snapshot still present");
		assert_eq!(reread.row_count, 99);
		assert_eq!(reread.read_version(), CommitVersion(42));
	}

	#[test]
	fn test_update_table_preserves_id_and_table_id() {
		let mut txn = create_test_admin_transaction();
		let created = CatalogStore::create_column_snapshot(
			&mut txn,
			ColumnSnapshotToCreate {
				namespace: NamespaceId(1),
				source: ColumnSnapshotSource::Table {
					table_id: TableId(50),
					commit_version: CommitVersion(5),
				},
				row_count: 1,
			},
		)
		.unwrap();

		let updated = CatalogStore::update_column_snapshot(
			&mut txn,
			created.id,
			ColumnSnapshotToUpdate {
				sequence_counter: 0,
				read_version: CommitVersion(99),
				row_count: 7,
			},
		)
		.unwrap();

		assert_eq!(updated.id, created.id);
		match updated.source {
			ColumnSnapshotSource::Table {
				table_id,
				commit_version,
			} => {
				assert_eq!(table_id, TableId(50), "table_id is immutable");
				assert_eq!(commit_version, CommitVersion(99));
			}
			other => panic!("expected Table source, got {other:?}"),
		}
	}

	#[test]
	fn test_update_nonexistent_column_snapshot_errors() {
		let mut txn = create_test_admin_transaction();
		let err = CatalogStore::update_column_snapshot(
			&mut txn,
			ColumnSnapshotId(999_999),
			ColumnSnapshotToUpdate {
				sequence_counter: 1,
				read_version: CommitVersion(1),
				row_count: 0,
			},
		)
		.expect_err("update of missing id must error");

		let diag = err.diagnostic();
		assert_eq!(diag.code, "INTERNAL_ERROR", "expected INTERNAL_ERROR diagnostic, got {}", diag.code);
		assert!(
			diag.message.contains("ColumnSnapshot with ID"),
			"diagnostic should name the entity, got: {}",
			diag.message
		);
	}
}
