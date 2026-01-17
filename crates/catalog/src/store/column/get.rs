// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::id::DictionaryId, key::columns::ColumnsKey};
use reifydb_transaction::standard::IntoStandardTransaction;
use reifydb_type::{
	error::Error,
	internal,
	value::{
		constraint::{Constraint, TypeConstraint},
		r#type::Type,
	},
};

use crate::store::column::schema::column::SCHEMA;

/// Decodes a constraint from stored bytes
fn decode_constraint(bytes: &[u8]) -> Option<Constraint> {
	if bytes.is_empty() {
		return None;
	}

	match bytes[0] {
		0 => None, // No constraint
		1 if bytes.len() >= 5 => {
			// MaxBytes constraint
			let max_bytes = u32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
			Some(Constraint::MaxBytes(max_bytes.into()))
		}
		2 if bytes.len() >= 3 => {
			// PrecisionScale constraint
			let precision = bytes[1];
			let scale = bytes[2];
			Some(Constraint::PrecisionScale(precision.into(), scale.into()))
		}
		_ => None, // Unknown or invalid constraint type
	}
}

use reifydb_core::interface::catalog::{
	column::{ColumnDef, ColumnIndex},
	id::ColumnId,
};

use crate::{
	CatalogStore,
	store::column::schema::column::{AUTO_INCREMENT, CONSTRAINT, DICTIONARY_ID, ID, INDEX, NAME, VALUE},
};

impl CatalogStore {
	pub(crate) fn get_column(rx: &mut impl IntoStandardTransaction, column: ColumnId) -> crate::Result<ColumnDef> {
		let mut txn = rx.into_standard_transaction();
		let multi = txn.get(&ColumnsKey::encoded(column))?.ok_or_else(|| {
			Error(internal!(
				"Table column with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				column
			))
		})?;

		let row = multi.values;

		let id = ColumnId(SCHEMA.get_u64(&row, ID));
		let name = SCHEMA.get_utf8(&row, NAME).to_string();
		let base_type = Type::from_u8(SCHEMA.get_u8(&row, VALUE));
		let index = ColumnIndex(SCHEMA.get_u8(&row, INDEX));
		let auto_increment = SCHEMA.get_bool(&row, AUTO_INCREMENT);

		// Reconstruct constraint from stored blob
		let constraint_bytes = SCHEMA.get_blob(&row, CONSTRAINT);
		let constraint = match decode_constraint(constraint_bytes.as_bytes()) {
			Some(c) => TypeConstraint::with_constraint(base_type, c),
			None => TypeConstraint::unconstrained(base_type),
		};

		// Read dictionary_id (0 means no dictionary)
		let dict_id_raw = SCHEMA.get_u64(&row, DICTIONARY_ID);
		let dictionary_id = if dict_id_raw == 0 {
			None
		} else {
			Some(DictionaryId(dict_id_raw))
		};

		let policies = Self::list_column_policies(&mut txn, id)?;

		Ok(ColumnDef {
			id,
			name,
			constraint,
			index,
			policies,
			auto_increment,
			dictionary_id,
		})
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::ColumnId;
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

	use crate::{CatalogStore, test_utils::create_test_column};

	#[test]
	fn test_ok() {
		let mut txn = create_test_command_transaction();
		create_test_column(&mut txn, "col_1", TypeConstraint::unconstrained(Type::Int1), vec![]);
		create_test_column(&mut txn, "col_2", TypeConstraint::unconstrained(Type::Int2), vec![]);
		create_test_column(&mut txn, "col_3", TypeConstraint::unconstrained(Type::Int4), vec![]);

		let result = CatalogStore::get_column(&mut txn, ColumnId(8194)).unwrap();

		assert_eq!(result.id, ColumnId(8194));
		assert_eq!(result.name, "col_2");
		assert_eq!(result.constraint.get_type(), Type::Int2);
		assert_eq!(result.auto_increment, false);
	}

	#[test]
	fn test_not_found() {
		let mut txn = create_test_command_transaction();
		create_test_column(&mut txn, "col_1", TypeConstraint::unconstrained(Type::Int1), vec![]);
		create_test_column(&mut txn, "col_2", TypeConstraint::unconstrained(Type::Int2), vec![]);
		create_test_column(&mut txn, "col_3", TypeConstraint::unconstrained(Type::Int4), vec![]);

		let err = CatalogStore::get_column(&mut txn, ColumnId(4)).unwrap_err();
		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("ColumnId(4)"));
		assert!(err.message.contains("not found in catalog"));
	}
}
