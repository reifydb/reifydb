// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::interface::{QueryTransaction, SourceDef, SourceId};

use crate::{CatalogStore, table_virtual::VirtualTableRegistry};

impl CatalogStore {
	/// Find a source (table, store::view, or virtual table) by its SourceId
	/// Returns None if the source doesn't exist
	pub fn find_source(
		rx: &mut impl QueryTransaction,
		source: impl Into<SourceId>,
	) -> crate::Result<Option<SourceDef>> {
		let source_id = source.into();

		match source_id {
			SourceId::Table(table_id) => {
				if let Some(table) = Self::find_table(rx, table_id)? {
					Ok(Some(SourceDef::Table(table)))
				} else {
					Ok(None)
				}
			}
			SourceId::View(view_id) => {
				if let Some(view) = Self::find_view(rx, view_id)? {
					Ok(Some(SourceDef::View(view)))
				} else {
					Ok(None)
				}
			}
			SourceId::TableVirtual(table_virtual_id) => {
				if let Some(table_virtual) =
					VirtualTableRegistry::find_table_virtual(rx, table_virtual_id)?
				{
					// Convert Arc<TableVirtualDef> to TableVirtualDef
					let table_virtual_def =
						Arc::try_unwrap(table_virtual).unwrap_or_else(|arc| (*arc).clone());
					Ok(Some(SourceDef::TableVirtual(table_virtual_def)))
				} else {
					Ok(None)
				}
			}
			SourceId::RingBuffer(_ring_buffer_id) => {
				// TODO: Implement find_ring_buffer when ring
				// buffer catalog is ready For now, ring
				// buffers are not yet queryable
				Ok(None)
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{SourceDef, SourceId, TableId, TableVirtualId, ViewId};
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::{Type, TypeConstraint};

	use crate::{
		CatalogStore,
		store::view::{ViewColumnToCreate, ViewToCreate},
		test_utils::{ensure_test_namespace, ensure_test_table},
	};

	#[test]
	fn test_find_source_table() {
		let mut txn = create_test_command_transaction();
		let table = ensure_test_table(&mut txn);

		// Find source by TableId
		let source = CatalogStore::find_source(&mut txn, table.id).unwrap().expect("Source should exist");

		match source {
			SourceDef::Table(t) => {
				assert_eq!(t.id, table.id);
				assert_eq!(t.name, table.name);
			}
			_ => panic!("Expected table"),
		}

		// Find source by SourceId::Table
		let source = CatalogStore::find_source(&mut txn, SourceId::Table(table.id))
			.unwrap()
			.expect("Source should exist");

		match source {
			SourceDef::Table(t) => {
				assert_eq!(t.id, table.id);
			}
			_ => panic!("Expected table"),
		}
	}

	#[test]
	fn test_find_source_view() {
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

		// Find source by ViewId
		let source = CatalogStore::find_source(&mut txn, view.id).unwrap().expect("Source should exist");

		match source {
			SourceDef::View(v) => {
				assert_eq!(v.id, view.id);
				assert_eq!(v.name, view.name);
			}
			_ => panic!("Expected view"),
		}

		// Find source by SourceId::View
		let source = CatalogStore::find_source(&mut txn, SourceId::View(view.id))
			.unwrap()
			.expect("Source should exist");

		match source {
			SourceDef::View(v) => {
				assert_eq!(v.id, view.id);
			}
			_ => panic!("Expected view"),
		}
	}

	#[test]
	fn test_find_source_not_found() {
		let mut txn = create_test_command_transaction();

		// Non-existent table
		let source = CatalogStore::find_source(&mut txn, TableId(999)).unwrap();
		assert!(source.is_none());

		// Non-existent view
		let source = CatalogStore::find_source(&mut txn, ViewId(999)).unwrap();
		assert!(source.is_none());

		// Non-existent virtual table
		let source = CatalogStore::find_source(&mut txn, TableVirtualId(999)).unwrap();
		assert!(source.is_none());
	}

	#[test]
	fn test_find_source_table_virtual() {
		let mut txn = create_test_command_transaction();

		// Find the sequences virtual table
		let sequences_id = crate::system::ids::table_virtual::SEQUENCES;
		let source = CatalogStore::find_source(&mut txn, sequences_id)
			.unwrap()
			.expect("Sequences virtual table should exist");

		match source {
			SourceDef::TableVirtual(tv) => {
				assert_eq!(tv.id, sequences_id);
				assert_eq!(tv.name, "sequences");
			}
			_ => panic!("Expected virtual table"),
		}

		// Find source by SourceId::TableVirtual
		let source = CatalogStore::find_source(&mut txn, SourceId::TableVirtual(sequences_id))
			.unwrap()
			.expect("Source should exist");

		match source {
			SourceDef::TableVirtual(tv) => {
				assert_eq!(tv.id, sequences_id);
			}
			_ => panic!("Expected virtual table"),
		}
	}
}
