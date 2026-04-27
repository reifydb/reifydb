// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{column_snapshot::ColumnSnapshot, id::ColumnSnapshotId},
	return_internal_error,
};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn get_column_snapshot(rx: &mut Transaction<'_>, id: ColumnSnapshotId) -> Result<ColumnSnapshot> {
		match Self::find_column_snapshot(rx, id)? {
			Some(snap) => Ok(snap),
			None => return_internal_error!(
				"ColumnSnapshot with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				id
			),
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		common::CommitVersion,
		interface::catalog::{
			column_snapshot::ColumnSnapshotSource,
			id::{ColumnSnapshotId, NamespaceId, TableId},
		},
	};
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::{CatalogStore, store::column_snapshot::create::ColumnSnapshotToCreate};

	#[test]
	fn test_get_column_snapshot_exists() {
		let mut txn = create_test_admin_transaction();
		let created = CatalogStore::create_column_snapshot(
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

		let result = CatalogStore::get_column_snapshot(&mut Transaction::Admin(&mut txn), created.id).unwrap();

		assert_eq!(result.id, created.id);
		assert_eq!(result.row_count, 42);
		assert_eq!(result.read_version(), CommitVersion(7));
	}

	#[test]
	fn test_get_column_snapshot_not_exists() {
		let mut txn = create_test_admin_transaction();
		let missing = ColumnSnapshotId(999_999);

		let err = CatalogStore::get_column_snapshot(&mut Transaction::Admin(&mut txn), missing)
			.expect_err("missing snapshot must error");

		let diag = err.diagnostic();
		assert_eq!(diag.code, "INTERNAL_ERROR", "expected INTERNAL_ERROR diagnostic, got {}", diag.code);
		assert!(
			diag.message.contains("ColumnSnapshot with ID"),
			"diagnostic message should name the entity, got: {}",
			diag.message
		);
		assert!(
			diag.message.contains("999999"),
			"diagnostic message should include the missing id, got: {}",
			diag.message
		);
	}
}
