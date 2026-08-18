// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::interface::catalog::object::{Object, ObjectId};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result, vtable::VTableRegistry};

impl CatalogStore {
	pub(crate) fn find_object(rx: &mut Transaction<'_>, object: impl Into<ObjectId>) -> Result<Option<Object>> {
		let object_id = object.into();

		match object_id {
			ObjectId::Table(table_id) => {
				if let Some(table) = Self::find_table(rx, table_id)? {
					Ok(Some(Object::Table(table)))
				} else {
					Ok(None)
				}
			}
			ObjectId::View(view_id) => {
				if let Some(view) = Self::find_view(rx, view_id)? {
					Ok(Some(Object::View(view)))
				} else {
					Ok(None)
				}
			}
			ObjectId::TableVirtual(vtable_id) => {
				if let Some(vtable) = VTableRegistry::find_vtable(rx, vtable_id)? {
					let vtable = Arc::try_unwrap(vtable).unwrap_or_else(|arc| (*arc).clone());
					Ok(Some(Object::TableVirtual(vtable)))
				} else {
					Ok(None)
				}
			}
			ObjectId::RingBuffer(_ringbuffer_id) => {
				// TODO: `Object` has no RingBuffer variant to return.
				Ok(None)
			}
			ObjectId::Dictionary(_dictionary_id) => {
				// TODO: `Object` has no Dictionary variant to return.
				Ok(None)
			}
			ObjectId::Series(_series_id) => {
				// TODO: `Object` has no Series variant to return.
				Ok(None)
			}
			ObjectId::Queue(_queue_id) => Ok(None),
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{
		id::{TableId, ViewId},
		object::{Object, ObjectId},
		vtable::VTableId,
	};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::{
		fragment::Fragment,
		value::{constraint::TypeConstraint, value_type::ValueType},
	};

	use crate::{
		CatalogStore,
		store::view::create::{ViewColumnToCreate, ViewStorage, ViewToCreate},
		system::ids::vtable::SEQUENCES,
		test_utils::{ensure_test_namespace, ensure_test_table},
	};

	#[test]
	fn test_find_object_table() {
		let mut txn = create_test_admin_transaction();
		let table = ensure_test_table(&mut txn);

		let object = CatalogStore::find_object(&mut Transaction::Admin(&mut txn), table.id)
			.unwrap()
			.expect("Object should exist");

		match object {
			Object::Table(t) => {
				assert_eq!(t.id, table.id);
				assert_eq!(t.name, table.name);
			}
			_ => panic!("Expected table"),
		}

		let object = CatalogStore::find_object(&mut Transaction::Admin(&mut txn), ObjectId::Table(table.id))
			.unwrap()
			.expect("Object should exist");

		match object {
			Object::Table(t) => {
				assert_eq!(t.id, table.id);
			}
			_ => panic!("Expected table"),
		}
	}

	#[test]
	fn test_find_object_view() {
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
				storage: ViewStorage::default(),
				sort: vec![],
			},
		)
		.unwrap();

		let object = CatalogStore::find_object(&mut Transaction::Admin(&mut txn), view.id())
			.unwrap()
			.expect("Object should exist");

		match object {
			Object::View(v) => {
				assert_eq!(v.id(), view.id());
				assert_eq!(v.name(), view.name());
			}
			_ => panic!("Expected view"),
		}

		let object = CatalogStore::find_object(&mut Transaction::Admin(&mut txn), ObjectId::View(view.id()))
			.unwrap()
			.expect("Object should exist");

		match object {
			Object::View(v) => {
				assert_eq!(v.id(), view.id());
			}
			_ => panic!("Expected view"),
		}
	}

	#[test]
	fn test_find_object_not_found() {
		let mut txn = create_test_admin_transaction();

		let result = CatalogStore::find_object(&mut Transaction::Admin(&mut txn), TableId(999)).unwrap();
		assert!(result.is_none());

		let result = CatalogStore::find_object(&mut Transaction::Admin(&mut txn), ViewId(999)).unwrap();
		assert!(result.is_none());

		let result = CatalogStore::find_object(&mut Transaction::Admin(&mut txn), VTableId(999)).unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_find_object_vtable() {
		let mut txn = create_test_admin_transaction();

		let sequences_id = SEQUENCES;
		let object = CatalogStore::find_object(&mut Transaction::Admin(&mut txn), sequences_id)
			.unwrap()
			.expect("Sequences virtual table should exist");

		match object {
			Object::TableVirtual(tv) => {
				assert_eq!(tv.id, sequences_id);
				assert_eq!(tv.name, "sequences");
			}
			_ => panic!("Expected virtual table"),
		}

		let object = CatalogStore::find_object(
			&mut Transaction::Admin(&mut txn),
			ObjectId::TableVirtual(sequences_id),
		)
		.unwrap()
		.expect("Object should exist");

		match object {
			Object::TableVirtual(tv) => {
				assert_eq!(tv.id, sequences_id);
			}
			_ => panic!("Expected virtual table"),
		}
	}
}
