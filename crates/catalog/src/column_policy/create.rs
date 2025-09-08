// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	diagnostic::catalog::table_column_policy_already_exists,
	interface::{
		ColumnPolicy, ColumnPolicyKey, ColumnPolicyKind,
		CommandTransaction, EncodableKey,
	},
	return_error,
};

use crate::{
	CatalogStore, column::ColumnId, column_policy::layout::column_policy,
	sequence::SystemSequence,
};

impl CatalogStore {
	pub(crate) fn create_column_policy(
		txn: &mut impl CommandTransaction,
		column: ColumnId,
		policy: ColumnPolicyKind,
	) -> crate::Result<ColumnPolicy> {
		let (policy_kind, _value_kind) = policy.to_u8();
		for existing in Self::list_column_policies(txn, column)? {
			let (existing_kind, _) = existing.policy.to_u8();
			if existing_kind == policy_kind {
				let column = Self::get_column(txn, column)?;

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
			&ColumnPolicyKey {
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
	use ColumnPolicyKind::Saturation;
	use ColumnSaturationPolicy::Error;
	use reifydb_core::interface::{
		ColumnPolicyKind, ColumnSaturationPolicy, TableId,
	};
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::{Type, TypeConstraint};

	use crate::{
		CatalogStore,
		column::{ColumnId, ColumnIndex, ColumnToCreate},
		test_utils::{create_test_column, ensure_test_table},
	};

	#[test]
	fn test_ok() {
		let mut txn = create_test_command_transaction();
		ensure_test_table(&mut txn);
		create_test_column(
			&mut txn,
			"col_1",
			TypeConstraint::unconstrained(Type::Int2),
			vec![],
		);

		let policy = Saturation(Error);

		let result = CatalogStore::create_column_policy(
			&mut txn,
			ColumnId(8193),
			policy.clone(),
		)
		.unwrap();
		assert_eq!(result.column, ColumnId(8193));
		assert_eq!(result.policy, policy);
	}

	#[test]
	fn test_create_column_policy_duplicate_error() {
		let mut txn = create_test_command_transaction();
		ensure_test_table(&mut txn);

		CatalogStore::create_column(
			&mut txn,
			TableId(1),
			ColumnToCreate {
				fragment: None,
				schema_name: "schema",
				table: TableId(1),
				table_name: "table",
				column: "col1".to_string(),
				constraint: TypeConstraint::unconstrained(
					Type::Int2,
				),
				if_not_exists: false,
				policies: vec![],
				index: ColumnIndex(0),
				auto_increment: false,
			},
		)
		.unwrap();

		let policy = Saturation(ColumnSaturationPolicy::Undefined);
		CatalogStore::create_column_policy(
			&mut txn,
			ColumnId(8193),
			policy.clone(),
		)
		.unwrap();

		let err = CatalogStore::create_column_policy(
			&mut txn,
			ColumnId(8193),
			policy.clone(),
		)
		.unwrap_err();
		let diagnostic = err.diagnostic();
		assert_eq!(diagnostic.code, "CA_008");
	}
}
