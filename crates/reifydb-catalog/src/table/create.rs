// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	diagnostic::catalog::table_already_exists,
	interface::{
		ColumnPolicyKind, CommandTransaction, EncodableKey, Key,
		SchemaId, SchemaTableKey, TableDef, TableId, TableKey,
	},
	return_error,
};
use reifydb_type::{OwnedFragment, Type};

use crate::{
	CatalogStore,
	column::ColumnIndex,
	sequence::SystemSequence,
	table::layout::{table, table_schema},
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
	pub schema: SchemaId,
	pub columns: Vec<TableColumnToCreate>,
}

impl CatalogStore {
	pub fn create_table(
		txn: &mut impl CommandTransaction,
		to_create: TableToCreate,
	) -> crate::Result<TableDef> {
		let schema_id = to_create.schema;

		if let Some(table) = CatalogStore::find_table_by_name(
			txn,
			schema_id,
			&to_create.table,
		)? {
			let schema = CatalogStore::get_schema(txn, schema_id)?;
			return_error!(table_already_exists(
				to_create.fragment,
				&schema.name,
				&table.name
			));
		}

		let table_id = SystemSequence::next_table_id(txn)?;
		Self::store_table(txn, table_id, schema_id, &to_create)?;
		Self::link_table_to_schema(
			txn,
			schema_id,
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
		table::LAYOUT.set_u64(&mut row, table::PRIMARY_KEY, 0u64); // Initialize with no primary key

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
		// Look up schema name for error messages
		let schema_name = Self::find_schema(txn, to_create.schema)?
			.map(|s| s.name)
			.unwrap_or_else(|| {
				format!("schema_{}", to_create.schema)
			});

		for (idx, column_to_create) in
			to_create.columns.into_iter().enumerate()
		{
			Self::create_column(
				txn,
				table,
				crate::column::ColumnToCreate {
					fragment: column_to_create
						.fragment
						.clone(),
					schema_name: &schema_name,
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
		CatalogStore,
		table::{TableToCreate, layout::table_schema},
		test_utils::ensure_test_schema,
	};

	#[test]
	fn test_create_table() {
		let mut txn = create_test_command_transaction();

		let test_schema = ensure_test_schema(&mut txn);

		let to_create = TableToCreate {
			schema: test_schema.id,
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

		let err = CatalogStore::create_table(&mut txn, to_create)
			.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_003");
	}

	#[test]
	fn test_table_linked_to_schema() {
		let mut txn = create_test_command_transaction();
		let test_schema = ensure_test_schema(&mut txn);

		let to_create = TableToCreate {
			schema: test_schema.id,
			table: "test_table".to_string(),
			columns: vec![],
			fragment: None,
		};

		CatalogStore::create_table(&mut txn, to_create).unwrap();

		let to_create = TableToCreate {
			schema: test_schema.id,
			table: "another_table".to_string(),
			columns: vec![],
			fragment: None,
		};

		CatalogStore::create_table(&mut txn, to_create).unwrap();

		let links = txn
			.range(SchemaTableKey::full_scan(test_schema.id))
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
}
