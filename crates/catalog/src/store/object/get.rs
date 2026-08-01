// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::object::{Object, ObjectId},
	internal,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::error::Error;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn get_object(rx: &mut Transaction<'_>, object: impl Into<ObjectId>) -> Result<Object> {
		let object_id = object.into();

		CatalogStore::find_object(rx, object_id)?.ok_or_else(|| {
			let object_type = match object_id {
				ObjectId::Table(_) => "Table",
				ObjectId::View(_) => "View",
				ObjectId::TableVirtual(_) => "TableVirtual",
				ObjectId::RingBuffer(_) => "RingBuffer",
				ObjectId::Dictionary(_) => "Dictionary",
				ObjectId::Series(_) => "Series",
				ObjectId::Queue(_) => "Queue",
			};

			Error(Box::new(internal!(
				"{} with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				object_type,
				object_id
			)))
		})
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{
		id::{TableId, ViewId},
		object::{Object, ObjectId},
	};
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::{
		fragment::Fragment,
		value::{constraint::TypeConstraint, value_type::ValueType},
	};

	use crate::{
		CatalogStore,
		store::view::create::{ViewColumnToCreate, ViewStorageConfig, ViewToCreate},
		test_utils::{ensure_test_namespace, ensure_test_table},
	};

	#[test]
	fn test_get_object_table() {
		let mut txn = create_test_admin_transaction();
		let table = ensure_test_table(&mut txn);

		let object = CatalogStore::get_object(&mut Transaction::Admin(&mut txn), table.id).unwrap();

		match object {
			Object::Table(t) => {
				assert_eq!(t.id, table.id);
				assert_eq!(t.name, table.name);
			}
			_ => panic!("Expected table"),
		}

		let object =
			CatalogStore::get_object(&mut Transaction::Admin(&mut txn), ObjectId::Table(table.id)).unwrap();

		match object {
			Object::Table(t) => {
				assert_eq!(t.id, table.id);
			}
			_ => panic!("Expected table"),
		}
	}

	#[test]
	fn test_get_object_view() {
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
					constraint: TypeConstraint::unconstrained(ValueType::Uint8),
					dictionary_id: None,
				}],
				storage: ViewStorageConfig::default(),
				sort: vec![],
			},
		)
		.unwrap();

		let object = CatalogStore::get_object(&mut Transaction::Admin(&mut txn), view.id()).unwrap();

		match object {
			Object::View(v) => {
				assert_eq!(v.id(), view.id());
				assert_eq!(v.name(), view.name());
			}
			_ => panic!("Expected view"),
		}

		let object =
			CatalogStore::get_object(&mut Transaction::Admin(&mut txn), ObjectId::View(view.id())).unwrap();

		match object {
			Object::View(v) => {
				assert_eq!(v.id(), view.id());
			}
			_ => panic!("Expected view"),
		}
	}

	#[test]
	fn test_get_object_not_found_table() {
		let mut txn = create_test_admin_transaction();

		let result = CatalogStore::get_object(&mut Transaction::Admin(&mut txn), TableId(999));
		assert!(result.is_err());

		let err = result.unwrap_err();
		assert!(err.to_string().contains("Table with ID"));
		assert!(err.to_string().contains("critical catalog inconsistency"));
	}

	#[test]
	fn test_get_object_not_found_view() {
		let mut txn = create_test_admin_transaction();

		let result = CatalogStore::get_object(&mut Transaction::Admin(&mut txn), ViewId(999));
		assert!(result.is_err());

		let err = result.unwrap_err();
		assert!(err.to_string().contains("View with ID"));
		assert!(err.to_string().contains("critical catalog inconsistency"));
	}
}
