// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{ColumnPolicy, ColumnPolicyId, ColumnPolicyKey, ColumnPolicyKind, QueryTransaction};

use crate::{
	CatalogStore,
	store::{column::ColumnId, column_policy::layout::column_policy},
};

impl CatalogStore {
	pub fn list_column_policies(
		rx: &mut impl QueryTransaction,
		column: ColumnId,
	) -> crate::Result<Vec<ColumnPolicy>> {
		Ok(rx.range(ColumnPolicyKey::full_scan(column))?
			.map(|multi| {
				let row = multi.values;
				let id = ColumnPolicyId(column_policy::LAYOUT.get_u64(&row, column_policy::ID));
				let column = ColumnId(column_policy::LAYOUT.get_u64(&row, column_policy::COLUMN));

				let policy = ColumnPolicyKind::from_u8(
					column_policy::LAYOUT.get_u8(&row, column_policy::POLICY),
					column_policy::LAYOUT.get_u8(&row, column_policy::VALUE),
				);

				ColumnPolicy {
					id,
					column,
					policy,
				}
			})
			.collect::<Vec<_>>())
	}

	pub fn list_column_policies_all(rx: &mut impl QueryTransaction) -> crate::Result<Vec<ColumnPolicy>> {
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

	#[test]
	fn test_ok() {
		let mut txn = create_test_command_transaction();
		ensure_test_table(&mut txn);

		CatalogStore::create_column(
			&mut txn,
			TableId(1),
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace",
				table: TableId(1),
				table_name: "test_table",
				column: "with_policy".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Int2),
				if_not_exists: false,
				policies: vec![Saturation(Undefined)],
				index: ColumnIndex(0),
				auto_increment: false,
			},
		)
		.unwrap();

		let column = CatalogStore::get_column(&mut txn, ColumnId(8193)).unwrap();

		let policies = CatalogStore::list_column_policies(&mut txn, column.id).unwrap();

		assert_eq!(policies.len(), 1);
		assert_eq!(policies[0].column, column.id);
		assert!(matches!(policies[0].policy, Saturation(Undefined)));
	}
}
