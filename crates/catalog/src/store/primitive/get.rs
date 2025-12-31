// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	Error,
	interface::{PrimitiveDef, PrimitiveId},
};
use reifydb_transaction::IntoStandardTransaction;
use reifydb_type::internal;

use crate::CatalogStore;

impl CatalogStore {
	/// Get a primitive (table or view) by its PrimitiveId
	/// Returns an error if the primitive doesn't exist
	pub async fn get_primitive(
		rx: &mut impl IntoStandardTransaction,
		primitive: impl Into<PrimitiveId>,
	) -> crate::Result<PrimitiveDef> {
		let primitive_id = primitive.into();

		CatalogStore::find_primitive(rx, primitive_id).await?.ok_or_else(|| {
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
mod tests {
	use reifydb_core::interface::{PrimitiveDef, PrimitiveId, TableId, ViewId};
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::{Type, TypeConstraint};

	use crate::{
		CatalogStore,
		store::view::{ViewColumnToCreate, ViewToCreate},
		test_utils::{ensure_test_namespace, ensure_test_table},
	};

	#[tokio::test]
	async fn test_get_primitive_table() {
		let mut txn = create_test_command_transaction().await;
		let table = ensure_test_table(&mut txn).await;

		// Get primitive by TableId
		let primitive = CatalogStore::get_primitive(&mut txn, table.id).await.unwrap();

		match primitive {
			PrimitiveDef::Table(t) => {
				assert_eq!(t.id, table.id);
				assert_eq!(t.name, table.name);
			}
			_ => panic!("Expected table"),
		}

		// Get primitive by PrimitiveId::Table
		let primitive = CatalogStore::get_primitive(&mut txn, PrimitiveId::Table(table.id)).await.unwrap();

		match primitive {
			PrimitiveDef::Table(t) => {
				assert_eq!(t.id, table.id);
			}
			_ => panic!("Expected table"),
		}
	}

	#[tokio::test]
	async fn test_get_primitive_view() {
		let mut txn = create_test_command_transaction().await;
		let namespace = ensure_test_namespace(&mut txn).await;

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
		.await
		.unwrap();

		// Get primitive by ViewId
		let primitive = CatalogStore::get_primitive(&mut txn, view.id).await.unwrap();

		match primitive {
			PrimitiveDef::View(v) => {
				assert_eq!(v.id, view.id);
				assert_eq!(v.name, view.name);
			}
			_ => panic!("Expected view"),
		}

		// Get primitive by PrimitiveId::View
		let primitive = CatalogStore::get_primitive(&mut txn, PrimitiveId::View(view.id)).await.unwrap();

		match primitive {
			PrimitiveDef::View(v) => {
				assert_eq!(v.id, view.id);
			}
			_ => panic!("Expected view"),
		}
	}

	#[tokio::test]
	async fn test_get_primitive_not_found_table() {
		let mut txn = create_test_command_transaction().await;

		// Non-existent table should error
		let result = CatalogStore::get_primitive(&mut txn, TableId(999)).await;
		assert!(result.is_err());

		let err = result.unwrap_err();
		assert!(err.to_string().contains("Table with ID"));
		assert!(err.to_string().contains("critical catalog inconsistency"));
	}

	#[tokio::test]
	async fn test_get_primitive_not_found_view() {
		let mut txn = create_test_command_transaction().await;

		// Non-existent view should error
		let result = CatalogStore::get_primitive(&mut txn, ViewId(999)).await;
		assert!(result.is_err());

		let err = result.unwrap_err();
		assert!(err.to_string().contains("View with ID"));
		assert!(err.to_string().contains("critical catalog inconsistency"));
	}
}
