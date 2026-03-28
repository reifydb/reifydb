// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::interface::catalog::schema::{Schema, SchemaId};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result, vtable::VTableRegistry};

impl CatalogStore {
	/// Find a object (table, store::view, or virtual table) by its SchemaId
	/// Returns None if the object doesn't exist
	pub(crate) fn find_schema(rx: &mut Transaction<'_>, object: impl Into<SchemaId>) -> Result<Option<Schema>> {
		let object_id = object.into();

		match object_id {
			SchemaId::Table(table_id) => {
				if let Some(table) = Self::find_table(rx, table_id)? {
					Ok(Some(Schema::Table(table)))
				} else {
					Ok(None)
				}
			}
			SchemaId::View(view_id) => {
				if let Some(view) = Self::find_view(rx, view_id)? {
					Ok(Some(Schema::View(view)))
				} else {
					Ok(None)
				}
			}
			SchemaId::TableVirtual(vtable_id) => {
				if let Some(vtable) = VTableRegistry::find_vtable(rx, vtable_id)? {
					// Convert Arc<VTable> to VTable
					let vtable = Arc::try_unwrap(vtable).unwrap_or_else(|arc| (*arc).clone());
					Ok(Some(Schema::TableVirtual(vtable)))
				} else {
					Ok(None)
				}
			}
			SchemaId::RingBuffer(_ringbuffer_id) => {
				// TODO: Implement find_ringbuffer when ring
				// buffer catalog is ready For now, ring
				// buffers are not yet queryable
				Ok(None)
			}
			SchemaId::Dictionary(_dictionary_id) => {
				// TODO: Implement find_dictionary when dictionary
				// catalog is ready For now, dictionaries return
				// None as they use a different retrieval mechanism
				Ok(None)
			}
			SchemaId::Series(_series_id) => {
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
		schema::{Schema, SchemaId},
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
	fn test_find_schema_table() {
		let mut txn = create_test_admin_transaction();
		let table = ensure_test_table(&mut txn);

		// Find object by TableId
		let object = CatalogStore::find_schema(&mut Transaction::Admin(&mut txn), table.id)
			.unwrap()
			.expect("Schema should exist");

		match object {
			Schema::Table(t) => {
				assert_eq!(t.id, table.id);
				assert_eq!(t.name, table.name);
			}
			_ => panic!("Expected table"),
		}

		// Find object by SchemaId::Table
		let object = CatalogStore::find_schema(&mut Transaction::Admin(&mut txn), SchemaId::Table(table.id))
			.unwrap()
			.expect("Schema should exist");

		match object {
			Schema::Table(t) => {
				assert_eq!(t.id, table.id);
			}
			_ => panic!("Expected table"),
		}
	}

	#[test]
	fn test_find_schema_view() {
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
		let object = CatalogStore::find_schema(&mut Transaction::Admin(&mut txn), view.id())
			.unwrap()
			.expect("Schema should exist");

		match object {
			Schema::View(v) => {
				assert_eq!(v.id(), view.id());
				assert_eq!(v.name(), view.name());
			}
			_ => panic!("Expected view"),
		}

		// Find object by SchemaId::View
		let object = CatalogStore::find_schema(&mut Transaction::Admin(&mut txn), SchemaId::View(view.id()))
			.unwrap()
			.expect("Schema should exist");

		match object {
			Schema::View(v) => {
				assert_eq!(v.id(), view.id());
			}
			_ => panic!("Expected view"),
		}
	}

	#[test]
	fn test_find_schema_not_found() {
		let mut txn = create_test_admin_transaction();

		// Non-existent table
		let object = CatalogStore::find_schema(&mut Transaction::Admin(&mut txn), TableId(999)).unwrap();
		assert!(object.is_none());

		// Non-existent view
		let object = CatalogStore::find_schema(&mut Transaction::Admin(&mut txn), ViewId(999)).unwrap();
		assert!(object.is_none());

		// Non-existent virtual table
		let object = CatalogStore::find_schema(&mut Transaction::Admin(&mut txn), VTableId(999)).unwrap();
		assert!(object.is_none());
	}

	#[test]
	fn test_find_schema_vtable() {
		let mut txn = create_test_admin_transaction();

		// Find the sequences virtual table
		let sequences_id = SEQUENCES;
		let object = CatalogStore::find_schema(&mut Transaction::Admin(&mut txn), sequences_id)
			.unwrap()
			.expect("Sequences virtual table should exist");

		match object {
			Schema::TableVirtual(tv) => {
				assert_eq!(tv.id, sequences_id);
				assert_eq!(tv.name, "sequences");
			}
			_ => panic!("Expected virtual table"),
		}

		// Find object by SchemaId::TableVirtual
		let object = CatalogStore::find_schema(
			&mut Transaction::Admin(&mut txn),
			SchemaId::TableVirtual(sequences_id),
		)
		.unwrap()
		.expect("Schema should exist");

		match object {
			Schema::TableVirtual(tv) => {
				assert_eq!(tv.id, sequences_id);
			}
			_ => panic!("Expected virtual table"),
		}
	}
}
