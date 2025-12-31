// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	diagnostic::catalog::{auto_increment_invalid_type, table_column_already_exists},
	interface::{ColumnKey, ColumnPolicyKind, ColumnsKey, DictionaryId, PrimitiveId, TableId},
	return_error,
};
use reifydb_transaction::StandardCommandTransaction;
use reifydb_type::{Constraint, Fragment, Type, TypeConstraint};

/// Encodes a constraint to a byte vector for storage
fn encode_constraint(constraint: &Option<Constraint>) -> Vec<u8> {
	match constraint {
		None => vec![0], // Type 0: No constraint
		Some(Constraint::MaxBytes(max_bytes)) => {
			let mut bytes = vec![1]; // Type 1: MaxBytes
			let max_value: u32 = (*max_bytes).into();
			bytes.extend_from_slice(&max_value.to_le_bytes());
			bytes
		}
		Some(Constraint::PrecisionScale(precision, scale)) => {
			vec![2, (*precision).into(), (*scale).into()] // Type 2: PrecisionScale
		}
	}
}

use crate::{
	CatalogStore,
	store::{
		column::{
			ColumnDef, ColumnIndex,
			layout::{
				column,
				column::{AUTO_INCREMENT, CONSTRAINT, DICTIONARY_ID, ID, INDEX, NAME, SOURCE, VALUE},
				source_column,
			},
		},
		sequence::SystemSequence,
	},
};

pub struct ColumnToCreate {
	pub fragment: Option<Fragment>,
	pub namespace_name: String,
	pub table: TableId,     // FIXME refactor to source: PrimitiveId
	pub table_name: String, // FIXME refactor to source_name
	pub column: String,
	pub constraint: TypeConstraint,
	pub if_not_exists: bool,
	pub policies: Vec<ColumnPolicyKind>,
	pub index: ColumnIndex,
	pub auto_increment: bool,
	pub dictionary_id: Option<DictionaryId>,
}

impl CatalogStore {
	pub(crate) async fn create_column(
		txn: &mut StandardCommandTransaction,
		source: impl Into<PrimitiveId>,
		column_to_create: ColumnToCreate,
	) -> crate::Result<ColumnDef> {
		let source = source.into();

		// FIXME policies
		if let Some(column) = Self::find_column_by_name(txn, source, &column_to_create.column).await? {
			return_error!(table_column_already_exists(
				Fragment::None,
				&column_to_create.namespace_name,
				&column_to_create.table_name,
				&column.name,
			));
		}

		// Validate auto_increment is only used with integer types
		if column_to_create.auto_increment {
			let base_type = column_to_create.constraint.get_type();
			let is_integer_type = matches!(
				base_type,
				Type::Int1
					| Type::Int2 | Type::Int4 | Type::Int8
					| Type::Int16 | Type::Uint1 | Type::Uint2
					| Type::Uint4 | Type::Uint8 | Type::Uint16
			);

			if !is_integer_type {
				return_error!(auto_increment_invalid_type(
					column_to_create.fragment.unwrap_or_else(|| Fragment::None),
					&column_to_create.column,
					base_type,
				));
			}
		}

		let id = SystemSequence::next_column_id(txn).await?;

		let mut row = column::LAYOUT.allocate();
		column::LAYOUT.set_u64(&mut row, ID, id);
		column::LAYOUT.set_u64(&mut row, SOURCE, source);
		column::LAYOUT.set_utf8(&mut row, NAME, &column_to_create.column);
		column::LAYOUT.set_u8(&mut row, VALUE, column_to_create.constraint.get_type().to_u8());
		column::LAYOUT.set_u8(&mut row, INDEX, column_to_create.index);
		column::LAYOUT.set_bool(&mut row, AUTO_INCREMENT, column_to_create.auto_increment);

		// Store constraint as encoded blob
		let constraint_bytes = encode_constraint(column_to_create.constraint.constraint());
		let blob = reifydb_type::Blob::from(constraint_bytes);
		column::LAYOUT.set_blob(&mut row, CONSTRAINT, &blob);

		// Store dictionary_id (0 means no dictionary)
		let dict_id_value = column_to_create.dictionary_id.map(|id| u64::from(id)).unwrap_or(0);
		column::LAYOUT.set_u64(&mut row, DICTIONARY_ID, dict_id_value);

		txn.set(&ColumnsKey::encoded(id), row).await?;

		let mut row = source_column::LAYOUT.allocate();
		source_column::LAYOUT.set_u64(&mut row, source_column::ID, id);
		source_column::LAYOUT.set_utf8(&mut row, source_column::NAME, &column_to_create.column);
		source_column::LAYOUT.set_u8(&mut row, source_column::INDEX, column_to_create.index);
		txn.set(&ColumnKey::encoded(source, id), row).await?;

		for policy in column_to_create.policies {
			Self::create_column_policy(txn, id, policy).await?;
		}

		Ok(ColumnDef {
			id,
			name: column_to_create.column,
			constraint: column_to_create.constraint,
			index: column_to_create.index,
			policies: Self::list_column_policies(txn, id).await?,
			auto_increment: column_to_create.auto_increment,
			dictionary_id: column_to_create.dictionary_id,
		})
	}
}

#[cfg(test)]
mod test {
	use reifydb_core::interface::{ColumnId, ColumnIndex, TableId};
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::{Type, TypeConstraint};

	use crate::{CatalogStore, column::ColumnToCreate, test_utils::ensure_test_table};

	#[tokio::test]
	async fn test_create_column() {
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
				column: "col_1".to_string(),
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

		CatalogStore::create_column(
			&mut txn,
			TableId(1),
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace".to_string(),
				table: TableId(1),
				table_name: "test_table".to_string(),
				column: "col_2".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Int2),
				if_not_exists: false,
				policies: vec![],
				index: ColumnIndex(1),
				auto_increment: false,
				dictionary_id: None,
			},
		)
		.await
		.unwrap();

		let column_1 = CatalogStore::get_column(&mut txn, ColumnId(8193)).await.unwrap();

		assert_eq!(column_1.id, 8193);
		assert_eq!(column_1.name, "col_1");
		assert_eq!(column_1.constraint.get_type(), Type::Boolean);
		assert_eq!(column_1.auto_increment, false);

		let column_2 = CatalogStore::get_column(&mut txn, ColumnId(8194)).await.unwrap();

		assert_eq!(column_2.id, 8194);
		assert_eq!(column_2.name, "col_2");
		assert_eq!(column_2.constraint.get_type(), Type::Int2);
		assert_eq!(column_2.auto_increment, false);
	}

	#[tokio::test]
	async fn test_create_column_with_auto_increment() {
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
				column: "id".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Uint8),
				if_not_exists: false,
				policies: vec![],
				index: ColumnIndex(0),
				auto_increment: true,
				dictionary_id: None,
			},
		)
		.await
		.unwrap();

		let column = CatalogStore::get_column(&mut txn, ColumnId(8193)).await.unwrap();

		assert_eq!(column.id, ColumnId(8193));
		assert_eq!(column.name, "id");
		assert_eq!(column.constraint.get_type(), Type::Uint8);
		assert_eq!(column.auto_increment, true);
	}

	#[tokio::test]
	async fn test_auto_increment_invalid_type() {
		let mut txn = create_test_command_transaction().await;
		ensure_test_table(&mut txn).await;

		// Try to create a text column with auto_increment

		let err = CatalogStore::create_column(
			&mut txn,
			TableId(1),
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace".to_string(),
				table: TableId(1),
				table_name: "test_table".to_string(),
				column: "name".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Utf8),
				if_not_exists: false,
				policies: vec![],
				index: ColumnIndex(0),
				auto_increment: true,
				dictionary_id: None,
			},
		)
		.await
		.unwrap_err();

		let diagnostic = err.diagnostic();
		assert_eq!(diagnostic.code, "CA_006");
		assert!(diagnostic.message.contains("auto increment is not supported for type"));

		// Try with bool type
		let err = CatalogStore::create_column(
			&mut txn,
			TableId(1),
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace".to_string(),
				table: TableId(1),
				table_name: "test_table".to_string(),
				column: "is_active".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Boolean),
				if_not_exists: false,
				policies: vec![],
				index: ColumnIndex(0),
				auto_increment: true,
				dictionary_id: None,
			},
		)
		.await
		.unwrap_err();

		assert_eq!(err.diagnostic().code, "CA_006");

		// Try with float type
		let err = CatalogStore::create_column(
			&mut txn,
			TableId(1),
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace".to_string(),
				table: TableId(1),
				table_name: "test_table".to_string(),
				column: "price".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Float8),
				if_not_exists: false,
				policies: vec![],
				index: ColumnIndex(0),
				auto_increment: true,
				dictionary_id: None,
			},
		)
		.await
		.unwrap_err();

		assert_eq!(err.diagnostic().code, "CA_006");
	}

	#[tokio::test]
	async fn test_column_already_exists() {
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
				column: "col_1".to_string(),
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

		// Tries to create a column with the same name again
		let err = CatalogStore::create_column(
			&mut txn,
			TableId(1),
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace".to_string(),
				table: TableId(1),
				table_name: "test_table".to_string(),
				column: "col_1".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Boolean),
				if_not_exists: false,
				policies: vec![],
				index: ColumnIndex(1),
				auto_increment: false,
				dictionary_id: None,
			},
		)
		.await
		.unwrap_err();

		let diagnostic = err.diagnostic();
		assert_eq!(diagnostic.code, "CA_005");
	}
}
