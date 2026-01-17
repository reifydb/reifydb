// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::primitive::{PrimitiveDef, PrimitiveId};
use reifydb_transaction::standard::IntoStandardTransaction;
use reifydb_type::{error::Error, internal};

use crate::CatalogStore;

impl CatalogStore {
	/// Get a primitive (table or view) by its PrimitiveId
	/// Returns an error if the primitive doesn't exist
	pub(crate) fn get_primitive(
		rx: &mut impl IntoStandardTransaction,
		primitive: impl Into<PrimitiveId>,
	) -> crate::Result<PrimitiveDef> {
		let primitive_id = primitive.into();

		CatalogStore::find_primitive(rx, primitive_id)?.ok_or_else(|| {
			let primitive_type = match primitive_id {
				PrimitiveId::Table(_) => "Table",
				PrimitiveId::View(_) => "View",
				PrimitiveId::Flow(_) => "Flow",
				PrimitiveId::TableVirtual(_) => "TableVirtual",
				PrimitiveId::RingBuffer(_) => "RingBuffer",
				PrimitiveId::Dictionary(_) => "Dictionary",
			};

			Error(internal!(
				"{} with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				primitive_type,
				primitive_id
			))
		})
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{
		id::{TableId, ViewId},
		primitive::{PrimitiveDef, PrimitiveId},
	};
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

	use crate::{
		CatalogStore,
		store::view::create::{ViewColumnToCreate, ViewToCreate},
		test_utils::{ensure_test_namespace, ensure_test_table},
	};

	#[test]
	fn test_get_primitive_table() {
		let mut txn = create_test_command_transaction();
		let table = ensure_test_table(&mut txn);

		// Get primitive by TableId
		let primitive = CatalogStore::get_primitive(&mut txn, table.id).unwrap();

		match primitive {
			PrimitiveDef::Table(t) => {
				assert_eq!(t.id, table.id);
				assert_eq!(t.name, table.name);
			}
			_ => panic!("Expected table"),
		}

		// Get primitive by PrimitiveId::Table
		let primitive = CatalogStore::get_primitive(&mut txn, PrimitiveId::Table(table.id)).unwrap();

		match primitive {
			PrimitiveDef::Table(t) => {
				assert_eq!(t.id, table.id);
			}
			_ => panic!("Expected table"),
		}
	}

	#[test]
	fn test_get_primitive_view() {
		let mut txn = create_test_command_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let view = CatalogStore::create_deferred_view(
			&mut txn,
			ViewToCreate {
				fragment: None,
				namespace: namespace.id,
				name: "test_view".to_string(),
				columns: vec![ViewColumnToCreate {
					name: "id".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					fragment: None,
				}],
			},
		)
		.unwrap();

		// Get primitive by ViewId
		let primitive = CatalogStore::get_primitive(&mut txn, view.id).unwrap();

		match primitive {
			PrimitiveDef::View(v) => {
				assert_eq!(v.id, view.id);
				assert_eq!(v.name, view.name);
			}
			_ => panic!("Expected view"),
		}

		// Get primitive by PrimitiveId::View
		let primitive = CatalogStore::get_primitive(&mut txn, PrimitiveId::View(view.id)).unwrap();

		match primitive {
			PrimitiveDef::View(v) => {
				assert_eq!(v.id, view.id);
			}
			_ => panic!("Expected view"),
		}
	}

	#[test]
	fn test_get_primitive_not_found_table() {
		let mut txn = create_test_command_transaction();

		// Non-existent table should error
		let result = CatalogStore::get_primitive(&mut txn, TableId(999));
		assert!(result.is_err());

		let err = result.unwrap_err();
		assert!(err.to_string().contains("Table with ID"));
		assert!(err.to_string().contains("critical catalog inconsistency"));
	}

	#[test]
	fn test_get_primitive_not_found_view() {
		let mut txn = create_test_command_transaction();

		// Non-existent view should error
		let result = CatalogStore::get_primitive(&mut txn, ViewId(999));
		assert!(result.is_err());

		let err = result.unwrap_err();
		assert!(err.to_string().contains("View with ID"));
		assert!(err.to_string().contains("critical catalog inconsistency"));
	}
}
