// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::interface::{QueryTransaction, StoreDef, StoreId};

use crate::{CatalogStore, table_virtual::VirtualTableRegistry};

impl CatalogStore {
	/// Find a store (table, view, or virtual table) by its StoreId
	/// Returns None if the store doesn't exist
	pub fn find_store(
		rx: &mut impl QueryTransaction,
		store: impl Into<StoreId>,
	) -> crate::Result<Option<StoreDef>> {
		let store_id = store.into();

		match store_id {
			StoreId::Table(table_id) => {
				if let Some(table) =
					Self::find_table(rx, table_id)?
				{
					Ok(Some(StoreDef::Table(table)))
				} else {
					Ok(None)
				}
			}
			StoreId::View(view_id) => {
				if let Some(view) =
					Self::find_view(rx, view_id)?
				{
					Ok(Some(StoreDef::View(view)))
				} else {
					Ok(None)
				}
			}
			StoreId::TableVirtual(table_virtual_id) => {
				if let Some(table_virtual) =
					VirtualTableRegistry::find_table_virtual(rx, table_virtual_id)?
				{
					// Convert Arc<TableVirtualDef> to TableVirtualDef
					let table_virtual_def = Arc::try_unwrap(table_virtual)
						.unwrap_or_else(|arc| (*arc).clone());
					Ok(Some(StoreDef::TableVirtual(table_virtual_def)))
				} else {
					Ok(None)
				}
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{
		StoreDef, StoreId, TableId, TableVirtualId, ViewId,
	};
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::Type;

	use crate::{
		CatalogStore,
		test_utils::{ensure_test_schema, ensure_test_table},
		view::{ViewColumnToCreate, ViewToCreate},
	};

	#[test]
	fn test_find_store_table() {
		let mut txn = create_test_command_transaction();
		let table = ensure_test_table(&mut txn);

		// Find store by TableId
		let store = CatalogStore::find_store(&mut txn, table.id)
			.unwrap()
			.expect("Store should exist");

		match store {
			StoreDef::Table(t) => {
				assert_eq!(t.id, table.id);
				assert_eq!(t.name, table.name);
			}
			_ => panic!("Expected table"),
		}

		// Find store by StoreId::Table
		let store = CatalogStore::find_store(
			&mut txn,
			StoreId::Table(table.id),
		)
		.unwrap()
		.expect("Store should exist");

		match store {
			StoreDef::Table(t) => {
				assert_eq!(t.id, table.id);
			}
			_ => panic!("Expected table"),
		}
	}

	#[test]
	fn test_find_store_view() {
		let mut txn = create_test_command_transaction();
		let schema = ensure_test_schema(&mut txn);

		let view = CatalogStore::create_deferred_view(
			&mut txn,
			ViewToCreate {
				fragment: None,
				schema: schema.id,
				name: "test_view".to_string(),
				columns: vec![ViewColumnToCreate {
					name: "id".to_string(),
					ty: Type::Uint8,
					fragment: None,
				}],
			},
		)
		.unwrap();

		// Find store by ViewId
		let store = CatalogStore::find_store(&mut txn, view.id)
			.unwrap()
			.expect("Store should exist");

		match store {
			StoreDef::View(v) => {
				assert_eq!(v.id, view.id);
				assert_eq!(v.name, view.name);
			}
			_ => panic!("Expected view"),
		}

		// Find store by StoreId::View
		let store = CatalogStore::find_store(
			&mut txn,
			StoreId::View(view.id),
		)
		.unwrap()
		.expect("Store should exist");

		match store {
			StoreDef::View(v) => {
				assert_eq!(v.id, view.id);
			}
			_ => panic!("Expected view"),
		}
	}

	#[test]
	fn test_find_store_not_found() {
		let mut txn = create_test_command_transaction();

		// Non-existent table
		let store = CatalogStore::find_store(&mut txn, TableId(999))
			.unwrap();
		assert!(store.is_none());

		// Non-existent view
		let store = CatalogStore::find_store(&mut txn, ViewId(999))
			.unwrap();
		assert!(store.is_none());

		// Non-existent virtual table
		let store =
			CatalogStore::find_store(&mut txn, TableVirtualId(999))
				.unwrap();
		assert!(store.is_none());
	}

	#[test]
	fn test_find_store_table_virtual() {
		let mut txn = create_test_command_transaction();

		// Find the sequences virtual table
		let sequences_id = crate::system::ids::table_virtual::SEQUENCES;
		let store = CatalogStore::find_store(&mut txn, sequences_id)
			.unwrap()
			.expect("Sequences virtual table should exist");

		match store {
			StoreDef::TableVirtual(tv) => {
				assert_eq!(tv.id, sequences_id);
				assert_eq!(tv.name, "sequences");
			}
			_ => panic!("Expected virtual table"),
		}

		// Find store by StoreId::TableVirtual
		let store = CatalogStore::find_store(
			&mut txn,
			StoreId::TableVirtual(sequences_id),
		)
		.unwrap()
		.expect("Store should exist");

		match store {
			StoreDef::TableVirtual(tv) => {
				assert_eq!(tv.id, sequences_id);
			}
			_ => panic!("Expected virtual table"),
		}
	}
}
