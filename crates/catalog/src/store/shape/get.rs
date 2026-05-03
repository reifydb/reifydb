// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::shape::{Shape, ShapeId},
	internal,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::error::Error;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn get_shape(rx: &mut Transaction<'_>, shape: impl Into<ShapeId>) -> Result<Shape> {
		let shape_id = shape.into();

		CatalogStore::find_shape(rx, shape_id)?.ok_or_else(|| {
			let shape_type = match shape_id {
				ShapeId::Table(_) => "Table",
				ShapeId::View(_) => "View",
				ShapeId::TableVirtual(_) => "TableVirtual",
				ShapeId::RingBuffer(_) => "RingBuffer",
				ShapeId::Dictionary(_) => "Dictionary",
				ShapeId::Series(_) => "Series",
			};

			Error(Box::new(internal!(
				"{} with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				shape_type,
				shape_id
			)))
		})
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{
		id::{TableId, ViewId},
		shape::{Shape, ShapeId},
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
	fn test_get_shape_table() {
		let mut txn = create_test_admin_transaction();
		let table = ensure_test_table(&mut txn);

		// Get shape by TableId
		let shape = CatalogStore::get_shape(&mut Transaction::Admin(&mut txn), table.id).unwrap();

		match shape {
			Shape::Table(t) => {
				assert_eq!(t.id, table.id);
				assert_eq!(t.name, table.name);
			}
			_ => panic!("Expected table"),
		}

		// Get shape by ShapeId::Table
		let shape =
			CatalogStore::get_shape(&mut Transaction::Admin(&mut txn), ShapeId::Table(table.id)).unwrap();

		match shape {
			Shape::Table(t) => {
				assert_eq!(t.id, table.id);
			}
			_ => panic!("Expected table"),
		}
	}

	#[test]
	fn test_get_shape_view() {
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

		// Get shape by ViewId
		let shape = CatalogStore::get_shape(&mut Transaction::Admin(&mut txn), view.id()).unwrap();

		match shape {
			Shape::View(v) => {
				assert_eq!(v.id(), view.id());
				assert_eq!(v.name(), view.name());
			}
			_ => panic!("Expected view"),
		}

		// Get shape by ShapeId::View
		let shape =
			CatalogStore::get_shape(&mut Transaction::Admin(&mut txn), ShapeId::View(view.id())).unwrap();

		match shape {
			Shape::View(v) => {
				assert_eq!(v.id(), view.id());
			}
			_ => panic!("Expected view"),
		}
	}

	#[test]
	fn test_get_shape_not_found_table() {
		let mut txn = create_test_admin_transaction();

		// Non-existent table should error
		let result = CatalogStore::get_shape(&mut Transaction::Admin(&mut txn), TableId(999));
		assert!(result.is_err());

		let err = result.unwrap_err();
		assert!(err.to_string().contains("Table with ID"));
		assert!(err.to_string().contains("critical catalog inconsistency"));
	}

	#[test]
	fn test_get_shape_not_found_view() {
		let mut txn = create_test_admin_transaction();

		// Non-existent view should error
		let result = CatalogStore::get_shape(&mut Transaction::Admin(&mut txn), ViewId(999));
		assert!(result.is_err());

		let err = result.unwrap_err();
		assert!(err.to_string().contains("View with ID"));
		assert!(err.to_string().contains("critical catalog inconsistency"));
	}
}
