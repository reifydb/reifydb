// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use column::LAYOUT;
use reifydb_core::{
	Error,
	interface::{ColumnsKey, EncodableKey, QueryTransaction},
};
use reifydb_type::{Constraint, Type, TypeConstraint, internal_error};

/// Decodes a constraint from stored bytes
fn decode_constraint(bytes: &[u8]) -> Option<Constraint> {
	if bytes.is_empty() {
		return None;
	}

	match bytes[0] {
		0 => None, // No constraint
		1 if bytes.len() >= 5 => {
			// MaxBytes constraint
			let max_bytes = u32::from_le_bytes([
				bytes[1], bytes[2], bytes[3], bytes[4],
			]) as usize;
			Some(Constraint::MaxBytes(max_bytes))
		}
		2 if bytes.len() >= 3 => {
			// PrecisionScale constraint
			let precision = bytes[1];
			let scale = bytes[2];
			Some(Constraint::PrecisionScale(precision, scale))
		}
		_ => None, // Unknown or invalid constraint type
	}
}

use crate::{
	CatalogStore,
	column::{ColumnDef, ColumnId, ColumnIndex, layout::column},
};

impl CatalogStore {
	pub fn get_column(
		rx: &mut impl QueryTransaction,
		column: ColumnId,
	) -> crate::Result<ColumnDef> {
		let versioned = rx
			.get(&ColumnsKey { column }.encode())?
			.ok_or_else(|| {
				Error(internal_error!(
					"Table column with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
					column
				))
			})?;

		let row = versioned.row;

		let id = ColumnId(LAYOUT.get_u64(&row, column::ID));
		let name = LAYOUT.get_utf8(&row, column::NAME).to_string();
		let base_type =
			Type::from_u8(LAYOUT.get_u8(&row, column::VALUE));
		let index = ColumnIndex(LAYOUT.get_u16(&row, column::INDEX));
		let auto_increment =
			LAYOUT.get_bool(&row, column::AUTO_INCREMENT);

		// Reconstruct constraint from stored blob
		let constraint_bytes =
			LAYOUT.get_blob(&row, column::CONSTRAINT);
		let constraint =
			match decode_constraint(constraint_bytes.as_bytes()) {
				Some(c) => TypeConstraint::with_constraint(
					base_type, c,
				),
				None => {
					TypeConstraint::unconstrained(base_type)
				}
			};

		let policies = Self::list_column_policies(rx, id)?;

		Ok(ColumnDef {
			id,
			name,
			constraint,
			index,
			policies,
			auto_increment,
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::{Type, TypeConstraint};

	use crate::{
		CatalogStore, column::ColumnId, test_utils::create_test_column,
	};

	#[test]
	fn test_ok() {
		let mut txn = create_test_command_transaction();
		create_test_column(
			&mut txn,
			"col_1",
			TypeConstraint::unconstrained(Type::Int1),
			vec![],
		);
		create_test_column(
			&mut txn,
			"col_2",
			TypeConstraint::unconstrained(Type::Int2),
			vec![],
		);
		create_test_column(
			&mut txn,
			"col_3",
			TypeConstraint::unconstrained(Type::Int4),
			vec![],
		);

		let result = CatalogStore::get_column(&mut txn, ColumnId(8194))
			.unwrap();

		assert_eq!(result.id, ColumnId(8194));
		assert_eq!(result.name, "col_2");
		assert_eq!(result.constraint.ty(), Type::Int2);
		assert_eq!(result.auto_increment, false);
	}

	#[test]
	fn test_not_found() {
		let mut txn = create_test_command_transaction();
		create_test_column(
			&mut txn,
			"col_1",
			TypeConstraint::unconstrained(Type::Int1),
			vec![],
		);
		create_test_column(
			&mut txn,
			"col_2",
			TypeConstraint::unconstrained(Type::Int2),
			vec![],
		);
		create_test_column(
			&mut txn,
			"col_3",
			TypeConstraint::unconstrained(Type::Int4),
			vec![],
		);

		let err = CatalogStore::get_column(&mut txn, ColumnId(4))
			.unwrap_err();
		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("ColumnId(4)"));
		assert!(err.message.contains("not found in catalog"));
	}
}
