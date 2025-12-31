// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	diagnostic::catalog::table_column_policy_already_exists,
	interface::{ColumnPolicy, ColumnPolicyKey, ColumnPolicyKind},
	return_error,
};
use reifydb_transaction::StandardCommandTransaction;

use crate::{
	CatalogStore,
	store::{column::ColumnId, column_policy::layout::column_policy, sequence::SystemSequence},
};

impl CatalogStore {
	pub(crate) async fn create_column_policy(
		txn: &mut StandardCommandTransaction,
		column: ColumnId,
		policy: ColumnPolicyKind,
	) -> crate::Result<ColumnPolicy> {
		let (policy_kind, _value_kind) = policy.to_u8();
		for existing in Self::list_column_policies(txn, column).await? {
			let (existing_kind, _) = existing.policy.to_u8();
			if existing_kind == policy_kind {
				let column = Self::get_column(txn, column).await?;

				return_error!(table_column_policy_already_exists(&policy.to_string(), &column.name));
			}
		}

		let id = SystemSequence::next_column_policy_id(txn).await?;

		let mut row = column_policy::LAYOUT.allocate();
		column_policy::LAYOUT.set_u64(&mut row, column_policy::ID, id);
		column_policy::LAYOUT.set_u64(&mut row, column_policy::COLUMN, column);

		{
			let (policy, value) = policy.to_u8();
			column_policy::LAYOUT.set_u8(&mut row, column_policy::POLICY, policy);
			column_policy::LAYOUT.set_u8(&mut row, column_policy::VALUE, value);
		}

		txn.set(&ColumnPolicyKey::encoded(column, id), row).await?;

		Ok(ColumnPolicy {
			id,
			column,
			policy,
		})
	}
}

#[cfg(test)]
mod tests {
	use ColumnPolicyKind::Saturation;
	use ColumnSaturationPolicy::Error;
	use reifydb_core::interface::{ColumnPolicyKind, ColumnSaturationPolicy, TableId};
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::{Type, TypeConstraint};

	use crate::{
		CatalogStore,
		store::column::{ColumnId, ColumnIndex, ColumnToCreate},
		test_utils::{create_test_column, ensure_test_table},
	};

	#[tokio::test]
	async fn test_ok() {
		let mut txn = create_test_command_transaction().await;
		ensure_test_table(&mut txn).await;
		create_test_column(&mut txn, "col_1", TypeConstraint::unconstrained(Type::Int2), vec![]).await;

		let policy = Saturation(Error);

		let result =
			CatalogStore::create_column_policy(&mut txn, ColumnId(8193), policy.clone()).await.unwrap();
		assert_eq!(result.column, ColumnId(8193));
		assert_eq!(result.policy, policy);
	}

	#[tokio::test]
	async fn test_create_column_policy_duplicate_error() {
		let mut txn = create_test_command_transaction().await;
		ensure_test_table(&mut txn).await;

		CatalogStore::create_column(
			&mut txn,
			TableId(1),
			ColumnToCreate {
				fragment: None,
				namespace_name: "namespace".to_string(),
				table: TableId(1),
				table_name: "table".to_string(),
				column: "col1".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Int2),
				if_not_exists: false,
				policies: vec![],
				index: ColumnIndex(0),
				auto_increment: false,
				dictionary_id: None,
			},
		)
		.await
		.unwrap();

		let policy = Saturation(ColumnSaturationPolicy::Undefined);
		CatalogStore::create_column_policy(&mut txn, ColumnId(8193), policy.clone()).await.unwrap();

		let err =
			CatalogStore::create_column_policy(&mut txn, ColumnId(8193), policy.clone()).await.unwrap_err();
		let diagnostic = err.diagnostic();
		assert_eq!(diagnostic.code, "CA_008");
	}
}
