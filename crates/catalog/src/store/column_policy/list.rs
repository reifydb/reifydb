// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		id::{ColumnId, ColumnPolicyId},
		policy::{ColumnPolicy, ColumnPolicyKind},
	},
	key::column_policy::ColumnPolicyKey,
};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, store::column_policy::schema::column_policy};

impl CatalogStore {
	pub(crate) fn list_column_policies(
		rx: &mut Transaction<'_>,
		column: ColumnId,
	) -> crate::Result<Vec<ColumnPolicy>> {
		let mut stream = rx.range(ColumnPolicyKey::full_scan(column), 1024)?;
		let mut result = Vec::new();

		while let Some(entry) = stream.next() {
			let multi = entry?;
			let row = multi.values;
			let id = ColumnPolicyId(column_policy::SCHEMA.get_u64(&row, column_policy::ID));
			let column = ColumnId(column_policy::SCHEMA.get_u64(&row, column_policy::COLUMN));

			let policy = ColumnPolicyKind::from_u8(
				column_policy::SCHEMA.get_u8(&row, column_policy::POLICY),
				column_policy::SCHEMA.get_u8(&row, column_policy::VALUE),
			);

			result.push(ColumnPolicy {
				id,
				column,
				policy,
			});
		}

		Ok(result)
	}

	pub(crate) fn list_column_policies_all(rx: &mut Transaction<'_>) -> crate::Result<Vec<ColumnPolicy>> {
		let mut result = Vec::new();

		// Get all columns from tables and views
		let columns = CatalogStore::list_columns_all(rx)?;

		// For each column, get its policies
		for info in columns {
			let policies = CatalogStore::list_column_policies(rx, info.column.id)?;
			result.extend(policies);
		}

		Ok(result)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{
		column::ColumnIndex,
		id::{ColumnId, TableId},
		policy::{ColumnPolicyKind, ColumnSaturationPolicy},
	};
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

	use crate::{CatalogStore, store::column::create::ColumnToCreate, test_utils::ensure_test_table};

	#[test]
	fn test_ok() {
		let mut txn = create_test_admin_transaction();
		ensure_test_table(&mut txn);

		CatalogStore::create_column(
			&mut txn,
			TableId(1),
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace".to_string(),
				primitive_name: "test_table".to_string(),
				column: "with_policy".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Int2),
				policies: vec![ColumnPolicyKind::Saturation(ColumnSaturationPolicy::None)],
				index: ColumnIndex(0),
				auto_increment: false,
				dictionary_id: None,
			},
		)
		.unwrap();

		let column = CatalogStore::get_column(&mut Transaction::Admin(&mut txn), ColumnId(8193)).unwrap();

		let policies =
			CatalogStore::list_column_policies(&mut Transaction::Admin(&mut txn), column.id).unwrap();

		assert_eq!(policies.len(), 1);
		assert_eq!(policies[0].column, column.id);
		assert!(matches!(policies[0].policy, ColumnPolicyKind::Saturation(ColumnSaturationPolicy::None)));
	}
}
