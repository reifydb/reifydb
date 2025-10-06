// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{CommandTransaction, Key, PrimaryKeyId, TableId, TableKey},
	return_internal_error,
};

use crate::{CatalogStore, store::table::layout::table};

impl CatalogStore {
	/// Set the primary key ID for a table
	/// Returns an internal error if the table doesn't exist
	pub fn set_table_primary_key(
		txn: &mut impl CommandTransaction,
		table_id: TableId,
		primary_key_id: PrimaryKeyId,
	) -> crate::Result<()> {
		let multi = match txn.get(&Key::Table(TableKey {
			table: table_id,
		})
		.encode())?
		{
			Some(v) => v,
			None => return_internal_error!(format!(
				"Table with ID {} not found when setting primary key. This indicates a critical catalog inconsistency.",
				table_id.0
			)),
		};

		let mut updated_row = multi.values.clone();
		table::LAYOUT.set_u64(&mut updated_row, table::PRIMARY_KEY, primary_key_id.0);

		txn.set(
			&Key::Table(TableKey {
				table: table_id,
			})
			.encode(),
			updated_row,
		)?;

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{PrimaryKeyId, TableId};
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{CatalogStore, test_utils::ensure_test_table};

	#[test]
	fn test_set_table_primary_key() {
		let mut txn = create_test_command_transaction();
		let table = ensure_test_table(&mut txn);

		// Set primary key
		CatalogStore::set_table_primary_key(&mut txn, table.id, PrimaryKeyId(42)).unwrap();

		// The test succeeds if no error is thrown.
		// In real usage, create_primary_key would create both the
		// PrimaryKey record and update the table, and
		// find_primary_key would find it.
	}

	#[test]
	fn test_set_table_primary_key_nonexistent() {
		let mut txn = create_test_command_transaction();

		// Try to set primary key on non-existent table
		let result = CatalogStore::set_table_primary_key(&mut txn, TableId(999), PrimaryKeyId(1));

		assert!(result.is_err());
		let err = result.unwrap_err();
		assert!(err.to_string().contains("Table with ID 999 not found"));
		assert!(err.to_string().contains("critical catalog inconsistency"));
	}
}
