// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{ColumnKey, QueryTransaction, SourceId};

use crate::{
	CatalogStore,
	store::column::{ColumnDef, ColumnId, layout::source_column},
};

/// Extended column information for system catalogs
pub struct ColumnInfo {
	pub column: ColumnDef,
	pub source_id: SourceId,
	pub is_view: bool,
}

impl CatalogStore {
	pub async fn list_columns(
		rx: &mut impl QueryTransaction,
		source: impl Into<SourceId>,
	) -> crate::Result<Vec<ColumnDef>> {
		let source = source.into();
		let mut result = vec![];

		let batch = rx.range(ColumnKey::full_scan(source)).await?;
		let ids: Vec<_> = batch
			.items
			.into_iter()
			.map(|multi| {
				let row = multi.values;
				ColumnId(source_column::LAYOUT.get_u64(&row, source_column::ID))
			})
			.collect();

		for id in ids {
			result.push(Self::get_column(rx, id).await?);
		}

		result.sort_by_key(|c| c.index);

		Ok(result)
	}

	pub async fn list_columns_all(rx: &mut impl QueryTransaction) -> crate::Result<Vec<ColumnInfo>> {
		let mut result = Vec::new();

		// Get all tables
		let tables = CatalogStore::list_tables_all(rx).await?;
		for table in tables {
			let columns = CatalogStore::list_columns(rx, table.id).await?;
			for column in columns {
				result.push(ColumnInfo {
					column,
					source_id: table.id.into(),
					is_view: false,
				});
			}
		}

		// Get all views
		let views = CatalogStore::list_views_all(rx).await?;
		for view in views {
			let columns = CatalogStore::list_columns(rx, view.id).await?;
			for column in columns {
				result.push(ColumnInfo {
					column,
					source_id: view.id.into(),
					is_view: true,
				});
			}
		}

		// Get all ring buffers
		let ringbuffers = CatalogStore::list_ringbuffers_all(rx).await?;
		for ringbuffer in ringbuffers {
			let columns = CatalogStore::list_columns(rx, ringbuffer.id).await?;
			for column in columns {
				result.push(ColumnInfo {
					column,
					source_id: ringbuffer.id.into(),
					is_view: false,
				});
			}
		}

		Ok(result)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::TableId;
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::{Type, TypeConstraint};

	use crate::{
		CatalogStore,
		store::column::{ColumnIndex, ColumnToCreate},
		test_utils::ensure_test_table,
	};

	#[tokio::test]
	async fn test_ok() {
		let mut txn = create_test_command_transaction().await;
		ensure_test_table(&mut txn).await;

		// Create columns out of order
		CatalogStore::create_column(
			&mut txn,
			TableId(1),
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace".to_string(),
				table: TableId(1),
				table_name: "test_table".to_string(),
				column: "b_col".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Int4),
				if_not_exists: false,
				policies: vec![],
				index: ColumnIndex(1),
				auto_increment: true,
				dictionary_id: None,
			},
		)
		.await
		.unwrap();

		CatalogStore::create_column(
			&mut txn,
			TableId(1),
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace".to_string(),
				table: TableId(1),
				table_name: "test_table".to_string(),
				column: "a_col".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Boolean),
				if_not_exists: false,
				policies: vec![],
				index: ColumnIndex(0),
				auto_increment: false,
				dictionary_id: None,
			},
		)
		.await
		.unwrap();

		let columns = CatalogStore::list_columns(&mut txn, TableId(1)).await.unwrap();
		assert_eq!(columns.len(), 2);

		assert_eq!(columns[0].name, "a_col"); // index 0
		assert_eq!(columns[1].name, "b_col"); // index 1

		assert_eq!(columns[0].index, ColumnIndex(0));
		assert_eq!(columns[1].index, ColumnIndex(1));

		assert_eq!(columns[0].auto_increment, false);
		assert_eq!(columns[1].auto_increment, true);
	}

	#[tokio::test]
	async fn test_empty() {
		let mut txn = create_test_command_transaction().await;
		ensure_test_table(&mut txn).await;

		let columns = CatalogStore::list_columns(&mut txn, TableId(1)).await.unwrap();
		assert!(columns.is_empty());
	}

	#[tokio::test]
	async fn test_table_does_not_exist() {
		let mut txn = create_test_command_transaction().await;

		let columns = CatalogStore::list_columns(&mut txn, TableId(1)).await.unwrap();
		assert!(columns.is_empty());
	}
}
