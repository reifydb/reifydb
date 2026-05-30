// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{column_snapshot::ColumnSnapshotSource, id::ColumnSnapshotId},
	key::column_snapshot::{ColumnSnapshotKey, SeriesColumnSnapshotKey, TableColumnSnapshotKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn drop_column_snapshot(txn: &mut AdminTransaction, id: ColumnSnapshotId) -> Result<()> {
		if let Some(snap) = Self::find_column_snapshot(&mut Transaction::Admin(&mut *txn), id)? {
			match snap.source {
				ColumnSnapshotSource::Table {
					table_id,
					..
				} => {
					txn.remove(&TableColumnSnapshotKey::encoded(table_id, id))?;
				}
				ColumnSnapshotSource::SeriesBucket {
					series_id,
					..
				} => {
					txn.remove(&SeriesColumnSnapshotKey::encoded(series_id, id))?;
				}
			}
		}

		txn.remove(&ColumnSnapshotKey::encoded(id))?;
		Ok(())
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
		key::column_snapshot::{SeriesColumnSnapshotKey, TableColumnSnapshotKey},
	};
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::{CatalogStore, store::column_snapshot::create::ColumnSnapshotToCreate};

	#[test]
	fn test_drop_table_column_snapshot_removes_primary_and_link_rows() {
		let mut txn = create_test_admin_transaction();
		let created = CatalogStore::create_column_snapshot(
			&mut txn,
			ColumnSnapshotToCreate {
				namespace: NamespaceId(1),
				source: ColumnSnapshotSource::Table {
					table_id: TableId(101),
					commit_version: CommitVersion(7),
				},
				row_count: 0,
			},
		)
		.unwrap();

		// Sanity: link row exists pre-drop.
		let link_pre = txn.get(&TableColumnSnapshotKey::encoded(TableId(101), created.id)).unwrap();
		assert!(link_pre.is_some(), "table link row should exist before drop");

		CatalogStore::drop_column_snapshot(&mut txn, created.id).unwrap();

		// Primary row gone.
		let found = CatalogStore::find_column_snapshot(&mut Transaction::Admin(&mut txn), created.id).unwrap();
		assert!(found.is_none(), "primary row should be removed");

		// Link row gone.
		let link_post = txn.get(&TableColumnSnapshotKey::encoded(TableId(101), created.id)).unwrap();
		assert!(link_post.is_none(), "table link row should be removed");
	}

	#[test]
	fn test_drop_series_bucket_column_snapshot_removes_link_row() {
		let mut txn = create_test_admin_transaction();
		let created = CatalogStore::create_column_snapshot(
			&mut txn,
			ColumnSnapshotToCreate {
				namespace: NamespaceId(1),
				source: ColumnSnapshotSource::SeriesBucket {
					series_id: SeriesId(202),
					bucket_start: 1000,
					bucket_width: 100,
					sequence_counter: 0,
					sealed_at_commit_version: CommitVersion(11),
				},
				row_count: 0,
			},
		)
		.unwrap();

		let link_pre = txn.get(&SeriesColumnSnapshotKey::encoded(SeriesId(202), created.id)).unwrap();
		assert!(link_pre.is_some());

		CatalogStore::drop_column_snapshot(&mut txn, created.id).unwrap();

		let found = CatalogStore::find_column_snapshot(&mut Transaction::Admin(&mut txn), created.id).unwrap();
		assert!(found.is_none());

		let link_post = txn.get(&SeriesColumnSnapshotKey::encoded(SeriesId(202), created.id)).unwrap();
		assert!(link_post.is_none(), "series link row should be removed");
	}

	#[test]
	fn test_drop_nonexistent_column_snapshot_succeeds() {
		let mut txn = create_test_admin_transaction();
		// Drop on an absent ID is a no-op like other catalog drops.
		let result = CatalogStore::drop_column_snapshot(&mut txn, ColumnSnapshotId(999_999));
		assert!(result.is_ok(), "drop of non-existent snapshot must not error: {:?}", result.err());
	}

	#[test]
	fn test_drop_does_not_affect_other_snapshots() {
		let mut txn = create_test_admin_transaction();
		let kept = CatalogStore::create_column_snapshot(
			&mut txn,
			ColumnSnapshotToCreate {
				namespace: NamespaceId(1),
				source: ColumnSnapshotSource::Table {
					table_id: TableId(50),
					commit_version: CommitVersion(5),
				},
				row_count: 0,
			},
		)
		.unwrap();
		let dropped = CatalogStore::create_column_snapshot(
			&mut txn,
			ColumnSnapshotToCreate {
				namespace: NamespaceId(1),
				source: ColumnSnapshotSource::Table {
					table_id: TableId(50),
					commit_version: CommitVersion(10),
				},
				row_count: 0,
			},
		)
		.unwrap();

		CatalogStore::drop_column_snapshot(&mut txn, dropped.id).unwrap();

		let still_present =
			CatalogStore::find_column_snapshot(&mut Transaction::Admin(&mut txn), kept.id).unwrap();
		assert!(still_present.is_some(), "untouched snapshot should remain");
	}
}
