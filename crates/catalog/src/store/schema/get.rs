// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::schema::{Schema, SchemaId},
	internal,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::error::Error;

use crate::{CatalogStore, Result};

impl CatalogStore {
	/// Get a object (table or view) by its SchemaId
	/// Returns an error if the object doesn't exist
	pub(crate) fn get_schema(rx: &mut Transaction<'_>, object: impl Into<SchemaId>) -> Result<Schema> {
		let object_id = object.into();

		CatalogStore::find_schema(rx, object_id)?.ok_or_else(|| {
			let schema_type = match object_id {
				SchemaId::Table(_) => "Table",
				SchemaId::View(_) => "View",
				SchemaId::TableVirtual(_) => "TableVirtual",
				SchemaId::RingBuffer(_) => "RingBuffer",
				SchemaId::Dictionary(_) => "Dictionary",
				SchemaId::Series(_) => "Series",
			};

			Error(internal!(
				"{} with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				schema_type,
				object_id
			))
		})
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{
		id::{TableId, ViewId},
		schema::{Schema, SchemaId},
	};
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_type::{
		fragment::Fragment,
		value::{constraint::TypeConstraint, r#type::Type},
	};

	use crate::{
		CatalogStore,
		store::view::create::{ViewColumnToCreate, ViewStorageConfig, ViewToCreate},
		test_utils::{ensure_test_namespace, ensure_test_table},
	};

	#[test]
	fn test_get_schema_table() {
		let mut txn = create_test_admin_transaction();
		let table = ensure_test_table(&mut txn);

		// Get object by TableId
		let object = CatalogStore::get_schema(&mut Transaction::Admin(&mut txn), table.id).unwrap();

		match object {
			Schema::Table(t) => {
				assert_eq!(t.id, table.id);
				assert_eq!(t.name, table.name);
			}
			_ => panic!("Expected table"),
		}

		// Get object by SchemaId::Table
		let object =
			CatalogStore::get_schema(&mut Transaction::Admin(&mut txn), SchemaId::Table(table.id)).unwrap();

		match object {
			Schema::Table(t) => {
				assert_eq!(t.id, table.id);
			}
			_ => panic!("Expected table"),
		}
	}

	#[test]
	fn test_get_schema_view() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let view = CatalogStore::create_deferred_view(
			&mut txn,
			ViewToCreate {
				name: Fragment::internal("test_view"),
				namespace: namespace.id(),
				columns: vec![ViewColumnToCreate {
					name: Fragment::internal("id"),
					fragment: Fragment::None,
					constraint: TypeConstraint::unconstrained(Type::Uint8),
				}],
				storage: ViewStorageConfig::default(),
			},
		)
		.unwrap();

		// Get object by ViewId
		let object = CatalogStore::get_schema(&mut Transaction::Admin(&mut txn), view.id()).unwrap();

		match object {
			Schema::View(v) => {
				assert_eq!(v.id(), view.id());
				assert_eq!(v.name(), view.name());
			}
			_ => panic!("Expected view"),
		}

		// Get object by SchemaId::View
		let object =
			CatalogStore::get_schema(&mut Transaction::Admin(&mut txn), SchemaId::View(view.id())).unwrap();

		match object {
			Schema::View(v) => {
				assert_eq!(v.id(), view.id());
			}
			_ => panic!("Expected view"),
		}
	}

	#[test]
	fn test_get_schema_not_found_table() {
		let mut txn = create_test_admin_transaction();

		// Non-existent table should error
		let result = CatalogStore::get_schema(&mut Transaction::Admin(&mut txn), TableId(999));
		assert!(result.is_err());

		let err = result.unwrap_err();
		assert!(err.to_string().contains("Table with ID"));
		assert!(err.to_string().contains("critical catalog inconsistency"));
	}

	#[test]
	fn test_get_schema_not_found_view() {
		let mut txn = create_test_admin_transaction();

		// Non-existent view should error
		let result = CatalogStore::get_schema(&mut Transaction::Admin(&mut txn), ViewId(999));
		assert!(result.is_err());

		let err = result.unwrap_err();
		assert!(err.to_string().contains("View with ID"));
		assert!(err.to_string().contains("critical catalog inconsistency"));
	}
}
