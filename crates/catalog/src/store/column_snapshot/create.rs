// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::pod::EncodedPodRow;
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
	store::{column_snapshot::shape::column_snapshot, sequence::system::SystemSequence},
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
		let mut row = column_snapshot::allocate();
		column_snapshot::set_id(&mut row, u64::from(id));
		column_snapshot::set_namespace(&mut row, u64::from(to_create.namespace));
		column_snapshot::set_kind(&mut row, to_create.source.kind() as u8);

		match &to_create.source {
			ColumnSnapshotSource::Table {
				table_id,
				commit_version,
			} => {
				column_snapshot::set_source_id(&mut row, u64::from(*table_id));
				column_snapshot::set_bucket_start(&mut row, 0u64);
				column_snapshot::set_bucket_width(&mut row, 0u64);
				column_snapshot::set_sequence_counter(&mut row, 0u64);
				column_snapshot::set_read_version(&mut row, commit_version.0);
			}
			ColumnSnapshotSource::SeriesBucket {
				series_id,
				bucket_start,
				bucket_width,
				sequence_counter,
				sealed_at_commit_version,
			} => {
				column_snapshot::set_source_id(&mut row, u64::from(*series_id));
				column_snapshot::set_bucket_start(&mut row, *bucket_start);
				column_snapshot::set_bucket_width(&mut row, *bucket_width);
				column_snapshot::set_sequence_counter(&mut row, *sequence_counter);
				column_snapshot::set_read_version(&mut row, sealed_at_commit_version.0);
			}
		}

		column_snapshot::set_row_count(&mut row, to_create.row_count);

		txn.set(&ColumnSnapshotKey::encoded(id), row.freeze())?;
		Ok(())
	}

	pub(crate) fn link_column_snapshot(
		txn: &mut AdminTransaction,
		id: ColumnSnapshotId,
		source: &ColumnSnapshotSource,
	) -> Result<()> {
		let row = EncodedPodRow::new(&u64::from(id).to_be_bytes());

		match source {
			ColumnSnapshotSource::Table {
				table_id,
				..
			} => {
				txn.set(&TableColumnSnapshotKey::encoded(*table_id, id), row.into_bytes())?;
			}
			ColumnSnapshotSource::SeriesBucket {
				series_id,
				..
			} => {
				txn.set(&SeriesColumnSnapshotKey::encoded(*series_id, id), row.into_bytes())?;
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
	use reifydb_test_harness::engine::create_test_admin_transaction;

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
