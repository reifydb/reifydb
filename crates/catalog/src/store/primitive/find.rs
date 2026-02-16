// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::interface::catalog::primitive::{PrimitiveDef, PrimitiveId};
use reifydb_transaction::transaction::AsTransaction;

use crate::{CatalogStore, vtable::VTableRegistry};

impl CatalogStore {
	/// Find a primitive (table, store::view, or virtual table) by its PrimitiveId
	/// Returns None if the primitive doesn't exist
	pub(crate) fn find_primitive(
		rx: &mut impl AsTransaction,
		primitive: impl Into<PrimitiveId>,
	) -> crate::Result<Option<PrimitiveDef>> {
		let primitive_id = primitive.into();
		let mut txn = rx.as_transaction();

		match primitive_id {
			PrimitiveId::Table(table_id) => {
				if let Some(table) = Self::find_table(&mut txn, table_id)? {
					Ok(Some(PrimitiveDef::Table(table)))
				} else {
					Ok(None)
				}
			}
			PrimitiveId::View(view_id) => {
				if let Some(view) = Self::find_view(&mut txn, view_id)? {
					Ok(Some(PrimitiveDef::View(view)))
				} else {
					Ok(None)
				}
			}
			PrimitiveId::Flow(flow_id) => {
				if let Some(flow) = Self::find_flow(&mut txn, flow_id)? {
					Ok(Some(PrimitiveDef::Flow(flow)))
				} else {
					Ok(None)
				}
			}
			PrimitiveId::TableVirtual(vtable_id) => {
				if let Some(vtable) = VTableRegistry::find_vtable(&mut txn, vtable_id)? {
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
pub mod tests {
	use reifydb_core::interface::catalog::{
		id::{TableId, ViewId},
		primitive::{PrimitiveDef, PrimitiveId},
		vtable::VTableId,
	};
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_type::{
		fragment::Fragment,
		value::{constraint::TypeConstraint, r#type::Type},
	};

	use crate::{
		CatalogStore,
		store::view::create::{ViewColumnToCreate, ViewToCreate},
		test_utils::{ensure_test_namespace, ensure_test_table},
	};

	#[test]
	fn test_find_primitive_table() {
		let mut txn = create_test_admin_transaction();
		let table = ensure_test_table(&mut txn);

		// Find primitive by TableId
		let primitive =
			CatalogStore::find_primitive(&mut txn, table.id).unwrap().expect("Primitive should exist");

		match primitive {
			PrimitiveDef::Table(t) => {
				assert_eq!(t.id, table.id);
				assert_eq!(t.name, table.name);
			}
			_ => panic!("Expected table"),
		}

		// Find primitive by PrimitiveId::Table
		let primitive = CatalogStore::find_primitive(&mut txn, PrimitiveId::Table(table.id))
			.unwrap()
			.expect("Primitive should exist");

		match primitive {
			PrimitiveDef::Table(t) => {
				assert_eq!(t.id, table.id);
			}
			_ => panic!("Expected table"),
		}
	}

	#[test]
	fn test_find_primitive_view() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let view = CatalogStore::create_deferred_view(
			&mut txn,
			ViewToCreate {
				name: Fragment::internal("test_view"),
				namespace: namespace.id,
				columns: vec![ViewColumnToCreate {
					name: Fragment::internal("id"),
					fragment: Fragment::None,
					constraint: TypeConstraint::unconstrained(Type::Uint8),
				}],
			},
		)
		.unwrap();

		// Find primitive by ViewId
		let primitive =
			CatalogStore::find_primitive(&mut txn, view.id).unwrap().expect("Primitive should exist");

		match primitive {
			PrimitiveDef::View(v) => {
				assert_eq!(v.id, view.id);
				assert_eq!(v.name, view.name);
			}
			_ => panic!("Expected view"),
		}

		// Find primitive by PrimitiveId::View
		let primitive = CatalogStore::find_primitive(&mut txn, PrimitiveId::View(view.id))
			.unwrap()
			.expect("Primitive should exist");

		match primitive {
			PrimitiveDef::View(v) => {
				assert_eq!(v.id, view.id);
			}
			_ => panic!("Expected view"),
		}
	}

	#[test]
	fn test_find_primitive_not_found() {
		let mut txn = create_test_admin_transaction();

		// Non-existent table
		let primitive = CatalogStore::find_primitive(&mut txn, TableId(999)).unwrap();
		assert!(primitive.is_none());

		// Non-existent view
		let primitive = CatalogStore::find_primitive(&mut txn, ViewId(999)).unwrap();
		assert!(primitive.is_none());

		// Non-existent virtual table
		let primitive = CatalogStore::find_primitive(&mut txn, VTableId(999)).unwrap();
		assert!(primitive.is_none());
	}

	#[test]
	fn test_find_primitive_vtable() {
		let mut txn = create_test_admin_transaction();

		// Find the sequences virtual table
		let sequences_id = crate::system::ids::vtable::SEQUENCES;
		let primitive = CatalogStore::find_primitive(&mut txn, sequences_id)
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
