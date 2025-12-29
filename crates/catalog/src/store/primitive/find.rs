// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::interface::{PrimitiveDef, PrimitiveId, QueryTransaction};

use crate::{CatalogStore, vtable::VTableRegistry};

impl CatalogStore {
	/// Find a primitive (table, store::view, or virtual table) by its PrimitiveId
	/// Returns None if the primitive doesn't exist
	pub async fn find_primitive(
		rx: &mut impl QueryTransaction,
		primitive: impl Into<PrimitiveId>,
	) -> crate::Result<Option<PrimitiveDef>> {
		let primitive_id = primitive.into();

		match primitive_id {
			PrimitiveId::Table(table_id) => {
				if let Some(table) = Self::find_table(rx, table_id).await? {
					Ok(Some(PrimitiveDef::Table(table)))
				} else {
					Ok(None)
				}
			}
			PrimitiveId::View(view_id) => {
				if let Some(view) = Self::find_view(rx, view_id).await? {
					Ok(Some(PrimitiveDef::View(view)))
				} else {
					Ok(None)
				}
			}
			PrimitiveId::Flow(flow_id) => {
				if let Some(flow) = Self::find_flow(rx, flow_id).await? {
					Ok(Some(PrimitiveDef::Flow(flow)))
				} else {
					Ok(None)
				}
			}
			PrimitiveId::TableVirtual(vtable_id) => {
				if let Some(vtable) = VTableRegistry::find_vtable(rx, vtable_id)? {
					// Convert Arc<VTableDef> to VTableDef
					let vtable_def = Arc::try_unwrap(vtable).unwrap_or_else(|arc| (*arc).clone());
					Ok(Some(PrimitiveDef::TableVirtual(vtable_def)))
				} else {
					Ok(None)
				}
			}
			PrimitiveId::RingBuffer(_ringbuffer_id) => {
				// TODO: Implement find_ringbuffer when ring
				// buffer catalog is ready For now, ring
				// buffers are not yet queryable
				Ok(None)
			}
			PrimitiveId::Dictionary(_dictionary_id) => {
				// TODO: Implement find_dictionary when dictionary
				// catalog is ready For now, dictionaries return
				// None as they use a different retrieval mechanism
				Ok(None)
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{PrimitiveDef, PrimitiveId, TableId, VTableId, ViewId};
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::{Type, TypeConstraint};

	use crate::{
		CatalogStore,
		store::view::{ViewColumnToCreate, ViewToCreate},
		test_utils::{ensure_test_namespace, ensure_test_table},
	};

	#[tokio::test]
	async fn test_find_primitive_table() {
		let mut txn = create_test_command_transaction().await;
		let table = ensure_test_table(&mut txn).await;

		// Find primitive by TableId
		let primitive = CatalogStore::find_primitive(&mut txn, table.id)
			.await
			.unwrap()
			.expect("Primitive should exist");

		match primitive {
			PrimitiveDef::Table(t) => {
				assert_eq!(t.id, table.id);
				assert_eq!(t.name, table.name);
			}
			_ => panic!("Expected table"),
		}

		// Find primitive by PrimitiveId::Table
		let primitive = CatalogStore::find_primitive(&mut txn, PrimitiveId::Table(table.id))
			.await
			.unwrap()
			.expect("Primitive should exist");

		match primitive {
			PrimitiveDef::Table(t) => {
				assert_eq!(t.id, table.id);
			}
			_ => panic!("Expected table"),
		}
	}

	#[tokio::test]
	async fn test_find_primitive_view() {
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

		// Find primitive by ViewId
		let primitive =
			CatalogStore::find_primitive(&mut txn, view.id).await.unwrap().expect("Primitive should exist");

		match primitive {
			PrimitiveDef::View(v) => {
				assert_eq!(v.id, view.id);
				assert_eq!(v.name, view.name);
			}
			_ => panic!("Expected view"),
		}

		// Find primitive by PrimitiveId::View
		let primitive = CatalogStore::find_primitive(&mut txn, PrimitiveId::View(view.id))
			.await
			.unwrap()
			.expect("Primitive should exist");

		match primitive {
			PrimitiveDef::View(v) => {
				assert_eq!(v.id, view.id);
			}
			_ => panic!("Expected view"),
		}
	}

	#[tokio::test]
	async fn test_find_primitive_not_found() {
		let mut txn = create_test_command_transaction().await;

		// Non-existent table
		let primitive = CatalogStore::find_primitive(&mut txn, TableId(999)).await.unwrap();
		assert!(primitive.is_none());

		// Non-existent view
		let primitive = CatalogStore::find_primitive(&mut txn, ViewId(999)).await.unwrap();
		assert!(primitive.is_none());

		// Non-existent virtual table
		let primitive = CatalogStore::find_primitive(&mut txn, VTableId(999)).await.unwrap();
		assert!(primitive.is_none());
	}

	#[tokio::test]
	async fn test_find_primitive_vtable() {
		let mut txn = create_test_command_transaction().await;

		// Find the sequences virtual table
		let sequences_id = crate::system::ids::vtable::SEQUENCES;
		let primitive = CatalogStore::find_primitive(&mut txn, sequences_id)
			.await
			.unwrap()
			.expect("Sequences virtual table should exist");

		match primitive {
			PrimitiveDef::TableVirtual(tv) => {
				assert_eq!(tv.id, sequences_id);
				assert_eq!(tv.name, "sequences");
			}
			_ => panic!("Expected virtual table"),
		}

		// Find primitive by PrimitiveId::TableVirtual
		let primitive = CatalogStore::find_primitive(&mut txn, PrimitiveId::TableVirtual(sequences_id))
			.await
			.unwrap()
			.expect("Primitive should exist");

		match primitive {
			PrimitiveDef::TableVirtual(tv) => {
				assert_eq!(tv.id, sequences_id);
			}
			_ => panic!("Expected virtual table"),
		}
	}
}
