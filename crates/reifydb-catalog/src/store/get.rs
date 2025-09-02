// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Error,
	interface::{QueryTransaction, StoreDef, StoreId},
};
use reifydb_type::internal_error;

use crate::CatalogStore;

impl CatalogStore {
	/// Get a store (table or view) by its StoreId
	/// Returns an error if the store doesn't exist
	pub fn get_store(
		rx: &mut impl QueryTransaction,
		store: impl Into<StoreId>,
	) -> crate::Result<StoreDef> {
		let store_id = store.into();

		CatalogStore::find_store(rx, store_id)?.ok_or_else(|| {
			let store_type = match store_id {
				StoreId::Table(_) => "Table",
				StoreId::View(_) => "View",
				StoreId::TableVirtual(_) => "TableVirtual",
			};

			Error(internal_error!(
				"{} with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				store_type,
				store_id
			))
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{StoreDef, StoreId, TableId, ViewId};
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::Type;

	use crate::{
		CatalogStore,
		test_utils::{ensure_test_schema, ensure_test_table},
		view::{ViewColumnToCreate, ViewToCreate},
	};

	#[test]
	fn test_get_store_table() {
		let mut txn = create_test_command_transaction();
		let table = ensure_test_table(&mut txn);

		// Get store by TableId
		let store =
			CatalogStore::get_store(&mut txn, table.id).unwrap();

		match store {
			StoreDef::Table(t) => {
				assert_eq!(t.id, table.id);
				assert_eq!(t.name, table.name);
			}
			_ => panic!("Expected table"),
		}

		// Get store by StoreId::Table
		let store = CatalogStore::get_store(
			&mut txn,
			StoreId::Table(table.id),
		)
		.unwrap();

		match store {
			StoreDef::Table(t) => {
				assert_eq!(t.id, table.id);
			}
			_ => panic!("Expected table"),
		}
	}

	#[test]
	fn test_get_store_view() {
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

		// Get store by ViewId
		let store = CatalogStore::get_store(&mut txn, view.id).unwrap();

		match store {
			StoreDef::View(v) => {
				assert_eq!(v.id, view.id);
				assert_eq!(v.name, view.name);
			}
			_ => panic!("Expected view"),
		}

		// Get store by StoreId::View
		let store = CatalogStore::get_store(
			&mut txn,
			StoreId::View(view.id),
		)
		.unwrap();

		match store {
			StoreDef::View(v) => {
				assert_eq!(v.id, view.id);
			}
			_ => panic!("Expected view"),
		}
	}

	#[test]
	fn test_get_store_not_found_table() {
		let mut txn = create_test_command_transaction();

		// Non-existent table should error
		let result = CatalogStore::get_store(&mut txn, TableId(999));
		assert!(result.is_err());

		let err = result.unwrap_err();
		assert!(err.to_string().contains("Table with ID"));
		assert!(err
			.to_string()
			.contains("critical catalog inconsistency"));
	}

	#[test]
	fn test_get_store_not_found_view() {
		let mut txn = create_test_command_transaction();

		// Non-existent view should error
		let result = CatalogStore::get_store(&mut txn, ViewId(999));
		assert!(result.is_err());

		let err = result.unwrap_err();
		assert!(err.to_string().contains("View with ID"));
		assert!(err
			.to_string()
			.contains("critical catalog inconsistency"));
	}
}
