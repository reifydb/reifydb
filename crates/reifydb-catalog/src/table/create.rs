// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	sequence::SystemSequence,
	table::layout::{table, table_schema},
	table_column::ColumnIndex,
	CatalogStore,
};
use reifydb_core::interface::CommandTransaction;
use reifydb_core::{
	interface::{
		ColumnPolicyKind, EncodableKey, Key, SchemaId, SchemaTableKey,
		TableDef, TableId, TableKey,
	}, result::error::diagnostic::catalog::{
		schema_not_found, table_already_exists,
	},
	return_error,
	OwnedFragment,
	Type,
};

#[derive(Debug, Clone)]
pub struct TableColumnToCreate {
	pub name: String,
	pub ty: Type,
	pub policies: Vec<ColumnPolicyKind>,
	pub auto_increment: bool,
	pub fragment: Option<OwnedFragment>,
}

#[derive(Debug, Clone)]
pub struct TableToCreate {
	pub fragment: Option<OwnedFragment>,
	pub table: String,
	pub schema: String,
	pub columns: Vec<TableColumnToCreate>,
}

impl CatalogStore {
	pub fn create_table(
		txn: &mut impl CommandTransaction,
		to_create: TableToCreate,
	) -> crate::Result<TableDef> {
		let Some(schema) =
			Self::find_schema_by_name(txn, &to_create.schema)?
		else {
			return_error!(schema_not_found(
				to_create.fragment,
				&to_create.schema
			));
		};

		if let Some(table) = Self::find_table_by_name(
			txn,
			schema.id,
			&to_create.table,
		)? {
			return_error!(table_already_exists(
				to_create.fragment,
				&schema.name,
				&table.name
			));
		}

		let table_id = SystemSequence::next_table_id(txn)?;
		Self::store_table(txn, table_id, schema.id, &to_create)?;
		Self::link_table_to_schema(
			txn,
			schema.id,
			table_id,
			&to_create.table,
		)?;

		Self::insert_columns(txn, table_id, to_create)?;

		Ok(Self::get_table(txn, table_id)?)
	}

	fn store_table(
		txn: &mut impl CommandTransaction,
		table: TableId,
		schema: SchemaId,
		to_create: &TableToCreate,
	) -> crate::Result<()> {
		let mut row = table::LAYOUT.allocate_row();
		table::LAYOUT.set_u64(&mut row, table::ID, table);
		table::LAYOUT.set_u64(&mut row, table::SCHEMA, schema);
		table::LAYOUT.set_utf8(&mut row, table::NAME, &to_create.table);

		txn.set(
			&TableKey {
				table,
			}
			.encode(),
			row,
		)?;

		Ok(())
	}

	fn link_table_to_schema(
		txn: &mut impl CommandTransaction,
		schema: SchemaId,
		table: TableId,
		name: &str,
	) -> crate::Result<()> {
		let mut row = table_schema::LAYOUT.allocate_row();
		table_schema::LAYOUT.set_u64(&mut row, table_schema::ID, table);
		table_schema::LAYOUT.set_utf8(
			&mut row,
			table_schema::NAME,
			name,
		);
		txn.set(
			&Key::SchemaTable(SchemaTableKey {
				schema,
				table,
			})
			.encode(),
			row,
		)?;
		Ok(())
	}

	fn insert_columns(
		txn: &mut impl CommandTransaction,
		table: TableId,
		to_create: TableToCreate,
	) -> crate::Result<()> {
		for (idx, column_to_create) in
			to_create.columns.into_iter().enumerate()
		{
			Self::create_table_column(
				txn,
				table,
				crate::table_column::TableColumnToCreate {
					fragment: column_to_create
						.fragment
						.clone(),
					schema_name: &to_create.schema,
					table,
					table_name: &to_create.table,
					column: column_to_create.name,
					value: column_to_create.ty,
					if_not_exists: false,
					policies: column_to_create
						.policies
						.clone(),
					index: ColumnIndex(idx as u16),
					auto_increment: column_to_create
						.auto_increment,
				},
			)?;
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{
		SchemaId, SchemaTableKey, TableId, VersionedQueryTransaction,
	};
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		table::{layout::table_schema, TableToCreate},
		test_utils::ensure_test_schema,
		CatalogStore,
	};

	#[test]
	fn test_create_table() {
		let mut txn = create_test_command_transaction();

		ensure_test_schema(&mut txn);

		let to_create = TableToCreate {
			schema: "test_schema".to_string(),
			table: "test_table".to_string(),
			columns: vec![],
			fragment: None,
		};

		// First creation should succeed
		let result =
			CatalogStore::create_table(&mut txn, to_create.clone())
				.unwrap();
		assert_eq!(result.id, TableId(1025));
		assert_eq!(result.schema, SchemaId(1025));
		assert_eq!(result.name, "test_table");

		// Creating the same table again with `if_not_exists = false`
		// should return error
		let err = CatalogStore::create_table(&mut txn, to_create)
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_003");
	}

	#[test]
	fn test_table_linked_to_schema() {
		let mut txn = create_test_command_transaction();
		let schema = ensure_test_schema(&mut txn);

		let to_create = TableToCreate {
			schema: "test_schema".to_string(),
			table: "test_table".to_string(),
			columns: vec![],
			fragment: None,
		};

		CatalogStore::create_table(&mut txn, to_create).unwrap();

		let to_create = TableToCreate {
			schema: "test_schema".to_string(),
			table: "another_table".to_string(),
			columns: vec![],
			fragment: None,
		};

		CatalogStore::create_table(&mut txn, to_create).unwrap();

		let links = txn
			.range(SchemaTableKey::full_scan(schema.id))
			.unwrap()
			.collect::<Vec<_>>();
		assert_eq!(links.len(), 2);

		let link = &links[1];
		let row = &link.row;
		assert_eq!(
			table_schema::LAYOUT.get_u64(row, table_schema::ID),
			1025
		);
		assert_eq!(
			table_schema::LAYOUT.get_utf8(row, table_schema::NAME),
			"test_table"
		);

		let link = &links[0];
		let row = &link.row;
		assert_eq!(
			table_schema::LAYOUT.get_u64(row, table_schema::ID),
			1026
		);
		assert_eq!(
			table_schema::LAYOUT.get_utf8(row, table_schema::NAME),
			"another_table"
		);
	}

	#[test]
	fn test_create_table_missing_schema() {
		let mut txn = create_test_command_transaction();

		let to_create = TableToCreate {
			schema: "missing_schema".to_string(),
			table: "my_table".to_string(),
			columns: vec![],
			fragment: None,
		};

		let err = CatalogStore::create_table(&mut txn, to_create)
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_002");
	}
}
