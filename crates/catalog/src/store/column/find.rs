// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{ColumnKey, PrimitiveId, QueryTransaction};

use crate::{
	CatalogStore,
	store::column::{ColumnDef, ColumnId, layout::source_column},
};

impl CatalogStore {
	pub async fn find_column_by_name(
		rx: &mut impl QueryTransaction,
		source: impl Into<PrimitiveId>,
		column_name: &str,
	) -> crate::Result<Option<ColumnDef>> {
		let batch = rx.range(ColumnKey::full_scan(source)).await?;
		let maybe_id = batch.items.into_iter().find_map(|multi| {
			let row = multi.values;
			let column = ColumnId(source_column::LAYOUT.get_u64(&row, source_column::ID));
			let name = source_column::LAYOUT.get_utf8(&row, source_column::NAME);

			if name == column_name {
				Some(column)
			} else {
				None
			}
		});

		if let Some(id) = maybe_id {
			Ok(Some(Self::get_column(rx, id).await?))
		} else {
			Ok(None)
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{ColumnId, TableId};
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::{Type, TypeConstraint};

	use crate::{CatalogStore, test_utils::create_test_column};

	#[tokio::test]
	async fn test_ok() {
		let mut txn = create_test_command_transaction().await;
		create_test_column(&mut txn, "col_1", TypeConstraint::unconstrained(Type::Int1), vec![]).await;
		create_test_column(&mut txn, "col_2", TypeConstraint::unconstrained(Type::Int2), vec![]).await;
		create_test_column(&mut txn, "col_3", TypeConstraint::unconstrained(Type::Int4), vec![]).await;

		let result = CatalogStore::find_column_by_name(&mut txn, TableId(1), "col_3").await.unwrap().unwrap();

		assert_eq!(result.id, ColumnId(8195));
		assert_eq!(result.name, "col_3");
		assert_eq!(result.constraint.get_type(), Type::Int4);
		assert_eq!(result.auto_increment, false);
	}

	#[tokio::test]
	async fn test_not_found() {
		let mut txn = create_test_command_transaction().await;
		create_test_column(&mut txn, "col_1", TypeConstraint::unconstrained(Type::Int1), vec![]).await;

		let result = CatalogStore::find_column_by_name(&mut txn, TableId(1), "not_found").await.unwrap();

		assert!(result.is_none());
	}
}
