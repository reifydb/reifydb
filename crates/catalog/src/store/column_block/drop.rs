// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{column_block::ColumnBlockSource, id::ColumnBlockId},
	key::column::{ColumnBlockKey, SeriesColumnBlockKey, TableColumnBlockKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn drop_column_block(txn: &mut AdminTransaction, id: ColumnBlockId) -> Result<()> {
		if let Some(block) = Self::find_column_block(&mut Transaction::Admin(&mut *txn), id)? {
			match block.source {
				ColumnBlockSource::Table {
					table_id,
					..
				} => {
					txn.remove(&TableColumnBlockKey::new(table_id, id))?;
				}
				ColumnBlockSource::SeriesBucket {
					series_id,
					partition,
					..
				} => {
					txn.remove(&SeriesColumnBlockKey::new(
						series_id,
						partition.unwrap_or_default(),
						id,
					))?;
				}
			}
		}

		txn.remove(&ColumnBlockKey::new(id))?;
		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		common::CommitVersion,
		interface::catalog::{
			column_block::ColumnBlockSource,
			id::{ColumnBlockId, NamespaceId, SeriesId, TableId},
		},
		key::column::{SeriesColumnBlockKey, TableColumnBlockKey},
	};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::value::partition::Partition;

	use crate::{CatalogStore, store::column_block::create::ColumnBlockToCreate};

	#[test]
	fn test_drop_table_column_block_removes_primary_and_link_rows() {
		let mut txn = create_test_admin_transaction();
		let created = CatalogStore::create_column_block(
			&mut txn,
			ColumnBlockToCreate {
				namespace: NamespaceId(1),
				source: ColumnBlockSource::Table {
					table_id: TableId(101),
					commit_version: CommitVersion(7),
				},
				row_count: 0,
				partition_values: Vec::new(),
				stats: Vec::new(),
			},
		)
		.unwrap();

		let link_pre = txn.get(&TableColumnBlockKey::new(TableId(101), created.id)).unwrap();
		assert!(link_pre.is_some(), "table link row should exist before drop");

		CatalogStore::drop_column_block(&mut txn, created.id).unwrap();

		let found = CatalogStore::find_column_block(&mut Transaction::Admin(&mut txn), created.id).unwrap();
		assert!(found.is_none(), "primary row should be removed");

		let link_post = txn.get(&TableColumnBlockKey::new(TableId(101), created.id)).unwrap();
		assert!(link_post.is_none(), "table link row should be removed");
	}

	#[test]
	fn test_drop_series_bucket_column_block_removes_link_row() {
		let mut txn = create_test_admin_transaction();
		let created = CatalogStore::create_column_block(
			&mut txn,
			ColumnBlockToCreate {
				namespace: NamespaceId(1),
				source: ColumnBlockSource::SeriesBucket {
					series_id: SeriesId(202),
					bucket_start: 1000,
					bucket_width: 100,
					partition: None,
					sequence_counter: 0,
					sealed_at_commit_version: CommitVersion(11),
				},
				row_count: 0,
				partition_values: Vec::new(),
				stats: Vec::new(),
			},
		)
		.unwrap();

		let link_pre =
			txn.get(&SeriesColumnBlockKey::new(SeriesId(202), Partition::default(), created.id)).unwrap();
		assert!(link_pre.is_some());

		CatalogStore::drop_column_block(&mut txn, created.id).unwrap();

		let found = CatalogStore::find_column_block(&mut Transaction::Admin(&mut txn), created.id).unwrap();
		assert!(found.is_none());

		let link_post =
			txn.get(&SeriesColumnBlockKey::new(SeriesId(202), Partition::default(), created.id)).unwrap();
		assert!(link_post.is_none(), "series link row should be removed");
	}

	#[test]
	fn test_drop_nonexistent_column_block_succeeds() {
		let mut txn = create_test_admin_transaction();
		let result = CatalogStore::drop_column_block(&mut txn, ColumnBlockId(999_999));
		assert!(result.is_ok(), "drop of non-existent block must not error: {:?}", result.err());
	}

	#[test]
	fn test_drop_does_not_affect_other_blocks() {
		let mut txn = create_test_admin_transaction();
		let kept = CatalogStore::create_column_block(
			&mut txn,
			ColumnBlockToCreate {
				namespace: NamespaceId(1),
				source: ColumnBlockSource::Table {
					table_id: TableId(50),
					commit_version: CommitVersion(5),
				},
				row_count: 0,
				partition_values: Vec::new(),
				stats: Vec::new(),
			},
		)
		.unwrap();
		let dropped = CatalogStore::create_column_block(
			&mut txn,
			ColumnBlockToCreate {
				namespace: NamespaceId(1),
				source: ColumnBlockSource::Table {
					table_id: TableId(50),
					commit_version: CommitVersion(10),
				},
				row_count: 0,
				partition_values: Vec::new(),
				stats: Vec::new(),
			},
		)
		.unwrap();

		CatalogStore::drop_column_block(&mut txn, dropped.id).unwrap();

		let still_present =
			CatalogStore::find_column_block(&mut Transaction::Admin(&mut txn), kept.id).unwrap();
		assert!(still_present.is_some(), "untouched block should remain");
	}
}
