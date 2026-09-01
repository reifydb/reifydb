// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{
	interface::catalog::id::{PrimaryKeyId, TableId},
	key::catalog::TableKey,
	return_internal_error,
};
use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::{CatalogStore, Result, store::table::shape::table};

impl CatalogStore {
	pub(crate) fn set_table_primary_key(
		txn: &mut AdminTransaction,
		table_id: TableId,
		primary_key_id: PrimaryKeyId,
	) -> Result<()> {
		let multi = match txn.get(&TableKey::encoded(table_id))? {
			Some(v) => v,
			None => return_internal_error!(format!(
				"Table with ID {} not found when setting primary key. This indicates a critical catalog inconsistency.",
				table_id.0
			)),
		};

		let mut updated_row = EncodedCatalogRow::try_from(multi.bytes.clone())?.thaw();
		table::set_primary_key(&mut updated_row, primary_key_id.0);

		txn.set(&TableKey::encoded(table_id), updated_row.freeze())?;

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::{PrimaryKeyId, TableId};
	use reifydb_test_harness::engine::create_test_admin_transaction;

	use crate::{CatalogStore, test_utils::ensure_test_table};

	#[test]
	fn test_set_table_primary_key() {
		let mut txn = create_test_admin_transaction();
		let table = ensure_test_table(&mut txn);

		CatalogStore::set_table_primary_key(&mut txn, table.id, PrimaryKeyId(42)).unwrap();
	}

	#[test]
	fn test_set_table_primary_key_nonexistent() {
		let mut txn = create_test_admin_transaction();

		let result = CatalogStore::set_table_primary_key(&mut txn, TableId(999), PrimaryKeyId(1));

		assert!(result.is_err());
		let err = result.unwrap_err();
		assert!(err.to_string().contains("Table with ID 999 not found"));
		assert!(err.to_string().contains("critical catalog inconsistency"));
	}
}
