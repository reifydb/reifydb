// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	constraint::decode_constraint_info, reader::Reader, row::catalog::EncodedCatalogRow,
	typeinfo::decode_value_type,
};
use reifydb_core::{
	interface::catalog::{
		column::{Column, ColumnIndex},
		id::ColumnId,
	},
	internal,
	key::column::ColumnsKey,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	error::Error,
	value::{constraint::TypeConstraint, dictionary::DictionaryId},
};

use crate::{CatalogStore, Result, store::column::shape::column};

impl CatalogStore {
	pub(crate) fn get_column(rx: &mut Transaction<'_>, column: ColumnId) -> Result<Column> {
		let multi = rx.get(&[ColumnsKey::new(column).into()])?.pop().flatten().ok_or_else(|| {
			Error(Box::new(internal!(
				"Table column with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				column
			)))
		})?;

		let bytes = EncodedCatalogRow::try_from(multi.bytes)?;

		let id = ColumnId(column::get_id(&bytes));
		let name = column::get_name(&bytes).to_string();
		let index = ColumnIndex(column::get_index(&bytes));
		let auto_increment = column::get_auto_increment(&bytes);

		let dict_id_raw = column::get_dictionary_id(&bytes);
		let dictionary_id = if dict_id_raw == 0 {
			None
		} else {
			Some(DictionaryId(dict_id_raw))
		};

		let value_type = decode_value_type(&mut Reader::new(column::get_value_type(&bytes).as_bytes()))
			.unwrap_or_else(|error| panic!("invalid persisted type of column {id:?}: {error}"));
		let constraint =
			match decode_constraint_info(&mut Reader::new(column::get_constraint(&bytes).as_bytes()))
				.unwrap_or_else(|error| {
					panic!("invalid persisted constraint of column {id:?}: {error}")
				}) {
				Some(constraint) => TypeConstraint::with_constraint(value_type, constraint),
				None => TypeConstraint::unconstrained(value_type),
			};

		let properties = Self::list_column_properties(rx, id)?;

		Ok(Column {
			id,
			name,
			constraint,
			index,
			properties,
			auto_increment,
			dictionary_id,
		})
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::{ColumnId, TableId};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::value::{constraint::TypeConstraint, value_type::ValueType};

	use crate::{CatalogStore, test_utils::create_test_column};

	#[test]
	fn test_ok() {
		let mut txn = create_test_admin_transaction();
		create_test_column(&mut txn, "col_1", TypeConstraint::unconstrained(ValueType::Int1), vec![]);
		create_test_column(&mut txn, "col_2", TypeConstraint::unconstrained(ValueType::Int2), vec![]);
		create_test_column(&mut txn, "col_3", TypeConstraint::unconstrained(ValueType::Int4), vec![]);

		let result = CatalogStore::get_column(&mut Transaction::Admin(&mut txn), ColumnId(16386)).unwrap();

		assert_eq!(result.id, ColumnId(16386));
		assert_eq!(result.name, "col_2");
		assert_eq!(result.constraint.get_type(), ValueType::Int2);
		assert!(!result.auto_increment);
	}

	#[test]
	fn test_not_found() {
		let mut txn = create_test_admin_transaction();
		create_test_column(&mut txn, "col_1", TypeConstraint::unconstrained(ValueType::Int1), vec![]);
		create_test_column(&mut txn, "col_2", TypeConstraint::unconstrained(ValueType::Int2), vec![]);
		create_test_column(&mut txn, "col_3", TypeConstraint::unconstrained(ValueType::Int4), vec![]);

		let err = CatalogStore::get_column(&mut Transaction::Admin(&mut txn), ColumnId(4)).unwrap_err();
		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("ColumnId(4)"));
		assert!(err.message.contains("not found in catalog"));
	}

	#[test]
	fn test_digest_column_type_round_trips() {
		// Reading only the type tag byte either panics on kind 32 or loses inner type and accuracy.
		let mut txn = create_test_admin_transaction();
		let plain = ValueType::Digest {
			inner: Box::new(ValueType::Duration),
			accuracy: 10_000,
		};
		let optional = ValueType::Option(Box::new(ValueType::Digest {
			inner: Box::new(ValueType::Float8),
			accuracy: 12_500,
		}));
		create_test_column(&mut txn, "plain", TypeConstraint::unconstrained(plain.clone()), vec![]);
		create_test_column(&mut txn, "optional", TypeConstraint::unconstrained(optional.clone()), vec![]);

		let columns = CatalogStore::list_columns(&mut Transaction::Admin(&mut txn), TableId(1)).unwrap();

		let types: Vec<_> = columns.iter().map(|c| (c.name.as_str(), c.constraint.clone())).collect();
		assert_eq!(
			types,
			vec![
				("plain", TypeConstraint::unconstrained(plain)),
				("optional", TypeConstraint::unconstrained(optional))
			]
		);
	}
}
