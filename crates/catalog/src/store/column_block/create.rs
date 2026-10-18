// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::{catalog::EncodedCatalogRowBuilder, pod::EncodedPodRow};
use reifydb_core::{
	interface::catalog::{
		column_block::{ColumnBlock, ColumnBlockKind, ColumnBlockSource, ColumnStats},
		id::{ColumnBlockId, NamespaceId},
	},
	key::column::{ColumnBlockKey, SeriesColumnBlockKey, TableColumnBlockKey},
};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_value::value::{Value, partition::Partition};

use crate::{
	CatalogStore, Result,
	store::{
		column_block::shape::{column_block, serialize_partition_values, serialize_stats},
		sequence::system::SystemSequence,
	},
};

#[derive(Debug, Clone)]
pub struct ColumnBlockToCreate {
	pub namespace: NamespaceId,
	pub source: ColumnBlockSource,
	pub row_count: u64,
	pub partition_values: Vec<Value>,
	pub stats: Vec<ColumnStats>,
}

impl CatalogStore {
	pub(crate) fn create_column_block(
		txn: &mut AdminTransaction,
		to_create: ColumnBlockToCreate,
	) -> Result<ColumnBlock> {
		let id = SystemSequence::next_column_block_id(txn)?;
		Self::store_column_block(txn, id, &to_create)?;
		Self::link_column_block(txn, id, &to_create.source)?;

		Ok(ColumnBlock {
			id,
			namespace: to_create.namespace,
			source: to_create.source,
			row_count: to_create.row_count,
			partition_values: to_create.partition_values,
			stats: to_create.stats,
		})
	}

	pub(crate) fn store_column_block(
		txn: &mut AdminTransaction,
		id: ColumnBlockId,
		to_create: &ColumnBlockToCreate,
	) -> Result<()> {
		let mut row = column_block::allocate();
		column_block::set_id(&mut row, u64::from(id));
		column_block::set_namespace(&mut row, u64::from(to_create.namespace));
		column_block::set_kind(&mut row, to_create.source.kind() as u8);

		match &to_create.source {
			ColumnBlockSource::Table {
				table_id,
				commit_version,
			} => {
				column_block::set_source_id(&mut row, u64::from(*table_id));
				column_block::set_bucket_start(&mut row, 0u64);
				column_block::set_bucket_width(&mut row, 0u64);
				column_block::set_partition_hi_none(&mut row);
				column_block::set_partition_lo_none(&mut row);
				column_block::set_sequence_counter(&mut row, 0u64);
				column_block::set_read_version(&mut row, commit_version.0);
			}
			ColumnBlockSource::SeriesBucket {
				series_id,
				bucket_start,
				bucket_width,
				partition,
				sequence_counter,
				sealed_at_commit_version,
			} => {
				column_block::set_source_id(&mut row, u64::from(*series_id));
				column_block::set_bucket_start(&mut row, *bucket_start);
				column_block::set_bucket_width(&mut row, *bucket_width);
				set_partition(&mut row, *partition);
				column_block::set_sequence_counter(&mut row, *sequence_counter);
				column_block::set_read_version(&mut row, sealed_at_commit_version.0);
			}
		}

		column_block::set_row_count(&mut row, to_create.row_count);
		column_block::set_partition_values(&mut row, &serialize_partition_values(&to_create.partition_values));
		column_block::set_stats(&mut row, &serialize_stats(&to_create.stats));

		txn.set(&ColumnBlockKey::new(id), row.freeze())?;
		Ok(())
	}

	pub(crate) fn link_column_block(
		txn: &mut AdminTransaction,
		id: ColumnBlockId,
		source: &ColumnBlockSource,
	) -> Result<()> {
		let row = EncodedPodRow::new(&u64::from(id).to_be_bytes());

		match source {
			ColumnBlockSource::Table {
				table_id,
				..
			} => {
				txn.set(&TableColumnBlockKey::new(*table_id, id), row.into_bytes())?;
			}
			ColumnBlockSource::SeriesBucket {
				series_id,
				partition,
				..
			} => {
				txn.set(
					&SeriesColumnBlockKey::new(*series_id, partition.unwrap_or_default(), id),
					row.into_bytes(),
				)?;
			}
		}

		let _ = ColumnBlockKind::Table;

		Ok(())
	}
}

fn set_partition(row: &mut EncodedCatalogRowBuilder, partition: Option<Partition>) {
	match partition {
		Some(partition) => {
			column_block::set_partition_hi(row, (partition.0 >> 64) as u64);
			column_block::set_partition_lo(row, partition.0 as u64);
		}
		None => {
			column_block::set_partition_hi_none(row);
			column_block::set_partition_lo_none(row);
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		common::CommitVersion,
		interface::catalog::{
			column_block::ColumnBlockSource,
			id::{NamespaceId, SeriesId, TableId},
		},
	};
	use reifydb_test_harness::engine::create_test_admin_transaction;

	use crate::{CatalogStore, store::column_block::create::ColumnBlockToCreate};

	#[test]
	fn test_create_table_column_block() {
		let mut txn = create_test_admin_transaction();

		let block = CatalogStore::create_column_block(
			&mut txn,
			ColumnBlockToCreate {
				namespace: NamespaceId(1),
				source: ColumnBlockSource::Table {
					table_id: TableId(101),
					commit_version: CommitVersion(7),
				},
				row_count: 42,
				partition_values: Vec::new(),
				stats: Vec::new(),
			},
		)
		.unwrap();

		assert!(block.id.0 > 0);
		assert_eq!(block.namespace, NamespaceId(1));
		assert_eq!(block.row_count, 42);
		assert_eq!(block.read_version(), CommitVersion(7));
	}

	#[test]
	fn test_create_series_bucket_column_block() {
		let mut txn = create_test_admin_transaction();

		let block = CatalogStore::create_column_block(
			&mut txn,
			ColumnBlockToCreate {
				namespace: NamespaceId(1),
				source: ColumnBlockSource::SeriesBucket {
					series_id: SeriesId(202),
					bucket_start: 1000,
					bucket_width: 100,
					partition: None,
					sequence_counter: 5,
					sealed_at_commit_version: CommitVersion(11),
				},
				row_count: 50,
				partition_values: Vec::new(),
				stats: Vec::new(),
			},
		)
		.unwrap();

		assert!(block.id.0 > 0);
		assert_eq!(block.read_version(), CommitVersion(11));
		assert_eq!(block.source.series_bucket_range(), Some((1000, 1100)));
	}
}
