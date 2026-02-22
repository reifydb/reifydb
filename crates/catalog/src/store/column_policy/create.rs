// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		id::ColumnId,
		policy::{ColumnPolicy, ColumnPolicyKind},
	},
	key::column_policy::ColumnPolicyKey,
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{
	CatalogStore,
	error::CatalogError,
	store::{column_policy::schema::column_policy, sequence::system::SystemSequence},
};

impl CatalogStore {
	pub(crate) fn create_column_policy(
		txn: &mut AdminTransaction,
		column: ColumnId,
		policy: ColumnPolicyKind,
	) -> crate::Result<ColumnPolicy> {
		let (policy_kind, _value_kind) = policy.to_u8();
		for existing in Self::list_column_policies(&mut Transaction::Admin(&mut *txn), column)? {
			let (existing_kind, _) = existing.policy.to_u8();
			if existing_kind == policy_kind {
				let column = Self::get_column(&mut Transaction::Admin(&mut *txn), column)?;

				return Err(CatalogError::ColumnPolicyAlreadyExists {
					policy: policy.to_string(),
					column: column.name,
				}
				.into());
			}
		}

		let id = SystemSequence::next_column_policy_id(txn)?;

		let mut row = column_policy::SCHEMA.allocate();
		column_policy::SCHEMA.set_u64(&mut row, column_policy::ID, id);
		column_policy::SCHEMA.set_u64(&mut row, column_policy::COLUMN, column);

		{
			let (policy, value) = policy.to_u8();
			column_policy::SCHEMA.set_u8(&mut row, column_policy::POLICY, policy);
			column_policy::SCHEMA.set_u8(&mut row, column_policy::VALUE, value);
		}

		txn.set(&ColumnPolicyKey::encoded(column, id), row)?;

		Ok(ColumnPolicy {
			id,
			column,
			policy,
		})
	}
}

#[cfg(test)]
pub mod tests {
	use ColumnPolicyKind::Saturation;
	use ColumnSaturationPolicy::Error;
	use reifydb_core::interface::catalog::{
		column::ColumnIndex,
		id::{ColumnId, TableId},
		policy::{ColumnPolicyKind, ColumnSaturationPolicy},
	};
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

	use crate::{
		CatalogStore,
		store::column::create::ColumnToCreate,
		test_utils::{create_test_column, ensure_test_table},
	};

	#[test]
	fn test_ok() {
		let mut txn = create_test_admin_transaction();
		ensure_test_table(&mut txn);
		create_test_column(&mut txn, "col_1", TypeConstraint::unconstrained(Type::Int2), vec![]);

		let policy = Saturation(Error);

		let result = CatalogStore::create_column_policy(&mut txn, ColumnId(8193), policy.clone()).unwrap();
		assert_eq!(result.column, ColumnId(8193));
		assert_eq!(result.policy, policy);
	}

	#[test]
	fn test_create_column_policy_duplicate_error() {
		let mut txn = create_test_admin_transaction();
		ensure_test_table(&mut txn);

		CatalogStore::create_column(
			&mut txn,
			TableId(1),
			ColumnToCreate {
				fragment: None,
				namespace_name: "namespace".to_string(),
				primitive_name: "table".to_string(),
				column: "col1".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Int2),
				policies: vec![],
				index: ColumnIndex(0),
				auto_increment: false,
				dictionary_id: None,
			},
		)
		.unwrap();

		let policy = Saturation(ColumnSaturationPolicy::None);
		CatalogStore::create_column_policy(&mut txn, ColumnId(8193), policy.clone()).unwrap();

		let err = CatalogStore::create_column_policy(&mut txn, ColumnId(8193), policy.clone()).unwrap_err();
		let diagnostic = err.diagnostic();
		assert_eq!(diagnostic.code, "CA_008");
	}
}
