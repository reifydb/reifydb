// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::interface::catalog::shape::{Shape, ShapeId};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result, vtable::VTableRegistry};

impl CatalogStore {
	/// Find a object (table, store::view, or virtual table) by its ShapeId
	/// Returns None if the object doesn't exist
	pub(crate) fn find_shape(rx: &mut Transaction<'_>, object: impl Into<ShapeId>) -> Result<Option<Shape>> {
		let object_id = object.into();

		match object_id {
			ShapeId::Table(table_id) => {
				if let Some(table) = Self::find_table(rx, table_id)? {
					Ok(Some(Shape::Table(table)))
				} else {
					Ok(None)
				}
			}
			ShapeId::View(view_id) => {
				if let Some(view) = Self::find_view(rx, view_id)? {
					Ok(Some(Shape::View(view)))
				} else {
					Ok(None)
				}
			}
			ShapeId::TableVirtual(vtable_id) => {
				if let Some(vtable) = VTableRegistry::find_vtable(rx, vtable_id)? {
					// Convert Arc<VTable> to VTable
					let vtable = Arc::try_unwrap(vtable).unwrap_or_else(|arc| (*arc).clone());
					Ok(Some(Shape::TableVirtual(vtable)))
				} else {
					Ok(None)
				}
			}
			ShapeId::RingBuffer(_ringbuffer_id) => {
				// TODO: Implement find_ringbuffer when ring
				// buffer catalog is ready For now, ring
				// buffers are not yet queryable
				Ok(None)
			}
			ShapeId::Dictionary(_dictionary_id) => {
				// TODO: Implement find_dictionary when dictionary
				// catalog is ready For now, dictionaries return
				// None as they use a different retrieval mechanism
				Ok(None)
			}
			ShapeId::Series(_series_id) => {
				// TODO: Implement find_series when series
				// catalog is ready
				Ok(None)
			}
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{
		id::{TableId, ViewId},
		shape::{Shape, ShapeId},
		vtable::VTableId,
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
		system::ids::vtable::SEQUENCES,
		test_utils::{ensure_test_namespace, ensure_test_table},
	};

	#[test]
	fn test_find_shape_table() {
		let mut txn = create_test_admin_transaction();
		let table = ensure_test_table(&mut txn);

		// Find object by TableId
		let object = CatalogStore::find_shape(&mut Transaction::Admin(&mut txn), table.id)
			.unwrap()
			.expect("Shape should exist");

		match object {
			Shape::Table(t) => {
				assert_eq!(t.id, table.id);
				assert_eq!(t.name, table.name);
			}
			_ => panic!("Expected table"),
		}

		// Find object by ShapeId::Table
		let object = CatalogStore::find_shape(&mut Transaction::Admin(&mut txn), ShapeId::Table(table.id))
			.unwrap()
			.expect("Shape should exist");

		match object {
			Shape::Table(t) => {
				assert_eq!(t.id, table.id);
			}
			_ => panic!("Expected table"),
		}
	}

	#[test]
	fn test_find_shape_view() {
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

		// Find object by ViewId
		let object = CatalogStore::find_shape(&mut Transaction::Admin(&mut txn), view.id())
			.unwrap()
			.expect("Shape should exist");

		match object {
			Shape::View(v) => {
				assert_eq!(v.id(), view.id());
				assert_eq!(v.name(), view.name());
			}
			_ => panic!("Expected view"),
		}

		// Find object by ShapeId::View
		let object = CatalogStore::find_shape(&mut Transaction::Admin(&mut txn), ShapeId::View(view.id()))
			.unwrap()
			.expect("Shape should exist");

		match object {
			Shape::View(v) => {
				assert_eq!(v.id(), view.id());
			}
			_ => panic!("Expected view"),
		}
	}

	#[test]
	fn test_find_shape_not_found() {
		let mut txn = create_test_admin_transaction();

		// Non-existent table
		let object = CatalogStore::find_shape(&mut Transaction::Admin(&mut txn), TableId(999)).unwrap();
		assert!(object.is_none());

		// Non-existent view
		let object = CatalogStore::find_shape(&mut Transaction::Admin(&mut txn), ViewId(999)).unwrap();
		assert!(object.is_none());

		// Non-existent virtual table
		let object = CatalogStore::find_shape(&mut Transaction::Admin(&mut txn), VTableId(999)).unwrap();
		assert!(object.is_none());
	}

	#[test]
	fn test_find_shape_vtable() {
		let mut txn = create_test_admin_transaction();

		// Find the sequences virtual table
		let sequences_id = SEQUENCES;
		let object = CatalogStore::find_shape(&mut Transaction::Admin(&mut txn), sequences_id)
			.unwrap()
			.expect("Sequences virtual table should exist");

		match object {
			Shape::TableVirtual(tv) => {
				assert_eq!(tv.id, sequences_id);
				assert_eq!(tv.name, "sequences");
			}
			_ => panic!("Expected virtual table"),
		}

		// Find object by ShapeId::TableVirtual
		let object = CatalogStore::find_shape(
			&mut Transaction::Admin(&mut txn),
			ShapeId::TableVirtual(sequences_id),
		)
		.unwrap()
		.expect("Shape should exist");

		match object {
			Shape::TableVirtual(tv) => {
				assert_eq!(tv.id, sequences_id);
			}
			_ => panic!("Expected virtual table"),
		}
	}
}
