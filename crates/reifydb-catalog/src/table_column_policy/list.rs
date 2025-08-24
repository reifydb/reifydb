// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	ColumnPolicy, ColumnPolicyId, ColumnPolicyKind, QueryTransaction,
	TableColumnPolicyKey,
};

use crate::{
	table_column::ColumnId, table_column_policy::layout::column_policy,
	CatalogStore,
};

impl CatalogStore {
	pub fn list_table_column_policies(
		rx: &mut impl QueryTransaction,
		column: ColumnId,
	) -> crate::Result<Vec<ColumnPolicy>> {
		Ok(rx.range(TableColumnPolicyKey::full_scan(column))?
			.map(|versioned| {
				let row = versioned.row;
				let id = ColumnPolicyId(
					column_policy::LAYOUT.get_u64(
						&row,
						column_policy::ID,
					),
				);
				let column = ColumnId(
					column_policy::LAYOUT.get_u64(
						&row,
						column_policy::COLUMN,
					),
				);

				let policy = ColumnPolicyKind::from_u8(
					column_policy::LAYOUT.get_u8(
						&row,
						column_policy::POLICY,
					),
					column_policy::LAYOUT.get_u8(
						&row,
						column_policy::VALUE,
					),
				);

				ColumnPolicy {
					id,
					column,
					policy,
				}
			})
			.collect::<Vec<_>>())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		interface::{
			ColumnPolicyKind, ColumnSaturationPolicy, TableId,
		},
		Type,
	};
	use reifydb_engine::test_utils::create_test_command_transaction;
	use ColumnPolicyKind::Saturation;
	use ColumnSaturationPolicy::Undefined;

	use crate::{
		table_column::{ColumnId, ColumnIndex, TableColumnToCreate},
		test_utils::ensure_test_table,
		CatalogStore,
	};

	#[test]
	fn test_ok() {
		let mut txn = create_test_command_transaction();
		ensure_test_table(&mut txn);

		CatalogStore::create_table_column(
			&mut txn,
			TableId(1),
			TableColumnToCreate {
				fragment: None,
				schema_name: "test_schema",
				table: TableId(1),
				table_name: "test_table",
				column: "with_policy".to_string(),
				value: Type::Int2,
				if_not_exists: false,
				policies: vec![Saturation(Undefined)],
				index: ColumnIndex(0),
				auto_increment: false,
			},
		)
		.unwrap();

		let column =
			CatalogStore::get_table_column(&mut txn, ColumnId(1))
				.unwrap();

		let policies = CatalogStore::list_table_column_policies(
			&mut txn, column.id,
		)
		.unwrap();
		assert_eq!(policies.len(), 1);
		assert_eq!(policies[0].column, column.id);
		assert!(matches!(policies[0].policy, Saturation(Undefined)));
	}
}
