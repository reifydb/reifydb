// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	sequence::SystemSequence, table_column::ColumnId, table_column_policy::layout::column_policy,
	Catalog,
};
use reifydb_core::interface::LiteCommandTransaction;
use reifydb_core::{
	interface::{
		ColumnPolicy, ColumnPolicyKind, EncodableKey,
		TableColumnPolicyKey, VersionedCommandTransaction,
	},
	result::error::diagnostic::catalog::table_column_policy_already_exists,
	return_error,
};

impl Catalog {
	pub(crate) fn create_table_column_policy(
		&self,
		txn: &mut impl LiteCommandTransaction,
		column: ColumnId,
		policy: ColumnPolicyKind,
	) -> crate::Result<ColumnPolicy> {
		let (policy_kind, _value_kind) = policy.to_u8();
		for existing in self.list_table_column_policies(txn, column)? {
			let (existing_kind, _) = existing.policy.to_u8();
			if existing_kind == policy_kind {
				let column =
					self.get_table_column(txn, column)?;

				return_error!(
					table_column_policy_already_exists(
						&policy.to_string(),
						&column.name
					)
				);
			}
		}

		let id = SystemSequence::next_column_policy_id(txn)?;

		let mut row = column_policy::LAYOUT.allocate_row();
		column_policy::LAYOUT.set_u64(&mut row, column_policy::ID, id);
		column_policy::LAYOUT.set_u64(
			&mut row,
			column_policy::COLUMN,
			column,
		);

		{
			let (policy, value) = policy.to_u8();
			column_policy::LAYOUT.set_u8(
				&mut row,
				column_policy::POLICY,
				policy,
			);
			column_policy::LAYOUT.set_u8(
				&mut row,
				column_policy::VALUE,
				value,
			);
		}

		txn.set(
			&TableColumnPolicyKey {
				column,
				policy: id,
			}
			.encode(),
			row,
		)?;

		Ok(ColumnPolicy {
			id,
			column,
			policy,
		})
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
	use reifydb_transaction::test_utils::create_test_command_transaction;
	use ColumnPolicyKind::Saturation;
	use ColumnSaturationPolicy::Error;

	use crate::{
		table_column::{ColumnId, ColumnIndex, TableColumnToCreate},
		test_utils::{create_test_table_column, ensure_test_table},
		Catalog,
	};

	#[test]
	fn test_ok() {
		let mut txn = create_test_command_transaction();
		ensure_test_table(&mut txn);
		create_test_table_column(&mut txn, "col_1", Type::Int2, vec![]);

		let policy = Saturation(Error);
		let catalog = Catalog::new();
		let result = catalog
			.create_table_column_policy(
				&mut txn,
				ColumnId(1),
				policy.clone(),
			)
			.unwrap();
		assert_eq!(result.column, ColumnId(1));
		assert_eq!(result.policy, policy);
	}

	#[test]
	fn test_create_column_policy_duplicate_error() {
		let mut txn = create_test_command_transaction();
		ensure_test_table(&mut txn);

		let catalog = Catalog::new();
		catalog.create_table_column(
			&mut txn,
			TableId(1),
			TableColumnToCreate {
				fragment: None,
				schema_name: "schema",
				table: TableId(1),
				table_name: "table",
				column: "col1".to_string(),
				value: Type::Int2,
				if_not_exists: false,
				policies: vec![],
				index: ColumnIndex(0),
				auto_increment: false,
			},
		)
		.unwrap();

		let policy = Saturation(ColumnSaturationPolicy::Undefined);
		catalog.create_table_column_policy(
			&mut txn,
			ColumnId(1),
			policy.clone(),
		)
		.unwrap();

		let err = catalog
			.create_table_column_policy(
				&mut txn,
				ColumnId(1),
				policy.clone(),
			)
			.unwrap_err();
		let diagnostic = err.diagnostic();
		assert_eq!(diagnostic.code, "CA_008");
	}
}
