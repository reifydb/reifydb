// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use futures_util::StreamExt;
use reifydb_core::interface::{ColumnPolicy, ColumnPolicyId, ColumnPolicyKey, ColumnPolicyKind};
use reifydb_transaction::IntoStandardTransaction;

use crate::{
	CatalogStore,
	store::{column::ColumnId, column_policy::layout::column_policy},
};

impl CatalogStore {
	pub async fn list_column_policies(
		rx: &mut impl IntoStandardTransaction,
		column: ColumnId,
	) -> crate::Result<Vec<ColumnPolicy>> {
		let mut txn = rx.into_standard_transaction();
		let mut stream = txn.range(ColumnPolicyKey::full_scan(column), 1024)?;
		let mut result = Vec::new();

		while let Some(entry) = stream.next().await {
			let multi = entry?;
			let row = multi.values;
			let id = ColumnPolicyId(column_policy::LAYOUT.get_u64(&row, column_policy::ID));
			let column = ColumnId(column_policy::LAYOUT.get_u64(&row, column_policy::COLUMN));

			let policy = ColumnPolicyKind::from_u8(
				column_policy::LAYOUT.get_u8(&row, column_policy::POLICY),
				column_policy::LAYOUT.get_u8(&row, column_policy::VALUE),
			);

			result.push(ColumnPolicy {
				id,
				column,
				policy,
			});
		}

		Ok(result)
	}

	pub async fn list_column_policies_all(
		rx: &mut impl IntoStandardTransaction,
	) -> crate::Result<Vec<ColumnPolicy>> {
		let mut txn = rx.into_standard_transaction();
		let mut result = Vec::new();

		// Get all columns from tables and views
		let columns = CatalogStore::list_columns_all(&mut txn).await?;

		// For each column, get its policies
		for info in columns {
			let policies = CatalogStore::list_column_policies(&mut txn, info.column.id).await?;
			result.extend(policies);
		}

		Ok(result)
	}
}

#[cfg(test)]
mod tests {
	use ColumnPolicyKind::Saturation;
	use ColumnSaturationPolicy::Undefined;
	use reifydb_core::interface::{ColumnPolicyKind, ColumnSaturationPolicy, TableId};
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::{Type, TypeConstraint};

	use crate::{
		CatalogStore,
		store::column::{ColumnId, ColumnIndex, ColumnToCreate},
		test_utils::ensure_test_table,
	};

	#[tokio::test]
	async fn test_ok() {
		let mut txn = create_test_command_transaction().await;
		ensure_test_table(&mut txn).await;

		CatalogStore::create_column(
			&mut txn,
			TableId(1),
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace".to_string(),
				table: TableId(1),
				table_name: "test_table".to_string(),
				column: "with_policy".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Int2),
				if_not_exists: false,
				policies: vec![Saturation(Undefined)],
				index: ColumnIndex(0),
				auto_increment: false,
				dictionary_id: None,
			},
		)
		.await
		.unwrap();

		let column = CatalogStore::get_column(&mut txn, ColumnId(8193)).await.unwrap();

		let policies = CatalogStore::list_column_policies(&mut txn, column.id).await.unwrap();

		assert_eq!(policies.len(), 1);
		assert_eq!(policies[0].column, column.id);
		assert!(matches!(policies[0].policy, Saturation(Undefined)));
	}
}
