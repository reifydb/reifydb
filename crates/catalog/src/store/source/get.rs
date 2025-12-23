// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Error,
	interface::{QueryTransaction, SourceDef, SourceId},
};
use reifydb_type::internal;

use crate::CatalogStore;

impl CatalogStore {
	/// Get a source (table or view) by its SourceId
	/// Returns an error if the source doesn't exist
	pub async fn get_source(
		rx: &mut impl QueryTransaction,
		source: impl Into<SourceId>,
	) -> crate::Result<SourceDef> {
		let source_id = source.into();

		CatalogStore::find_source(rx, source_id).await?.ok_or_else(|| {
			let source_type = match source_id {
				SourceId::Table(_) => "Table",
				SourceId::View(_) => "View",
				SourceId::Flow(_) => "Flow",
				SourceId::TableVirtual(_) => "TableVirtual",
				SourceId::RingBuffer(_) => "RingBuffer",
				SourceId::Dictionary(_) => "Dictionary",
			};

			Error(internal!(
				"{} with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				source_type,
				source_id
			))
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{SourceDef, SourceId, TableId, ViewId};
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::{Type, TypeConstraint};

	use crate::{
		CatalogStore,
		store::view::{ViewColumnToCreate, ViewToCreate},
		test_utils::{ensure_test_namespace, ensure_test_table},
	};

	#[tokio::test]
	async fn test_get_source_table() {
		let mut txn = create_test_command_transaction().await;
		let table = ensure_test_table(&mut txn).await;

		// Get store by TableId
		let source = CatalogStore::get_source(&mut txn, table.id).await.unwrap();

		match source {
			SourceDef::Table(t) => {
				assert_eq!(t.id, table.id);
				assert_eq!(t.name, table.name);
			}
			_ => panic!("Expected table"),
		}

		// Get store by SourceId::Table
		let source = CatalogStore::get_source(&mut txn, SourceId::Table(table.id)).await.unwrap();

		match source {
			SourceDef::Table(t) => {
				assert_eq!(t.id, table.id);
			}
			_ => panic!("Expected table"),
		}
	}

	#[tokio::test]
	async fn test_get_source_view() {
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

		// Get store by ViewId
		let source = CatalogStore::get_source(&mut txn, view.id).await.unwrap();

		match source {
			SourceDef::View(v) => {
				assert_eq!(v.id, view.id);
				assert_eq!(v.name, view.name);
			}
			_ => panic!("Expected view"),
		}

		// Get store by SourceId::View
		let source = CatalogStore::get_source(&mut txn, SourceId::View(view.id)).await.unwrap();

		match source {
			SourceDef::View(v) => {
				assert_eq!(v.id, view.id);
			}
			_ => panic!("Expected view"),
		}
	}

	#[tokio::test]
	async fn test_get_source_not_found_table() {
		let mut txn = create_test_command_transaction().await;

		// Non-existent table should error
		let result = CatalogStore::get_source(&mut txn, TableId(999)).await;
		assert!(result.is_err());

		let err = result.unwrap_err();
		assert!(err.to_string().contains("Table with ID"));
		assert!(err.to_string().contains("critical catalog inconsistency"));
	}

	#[tokio::test]
	async fn test_get_source_not_found_view() {
		let mut txn = create_test_command_transaction().await;

		// Non-existent view should error
		let result = CatalogStore::get_source(&mut txn, ViewId(999)).await;
		assert!(result.is_err());

		let err = result.unwrap_err();
		assert!(err.to_string().contains("View with ID"));
		assert!(err.to_string().contains("critical catalog inconsistency"));
	}
}
