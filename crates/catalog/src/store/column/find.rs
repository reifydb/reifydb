// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{column::ColumnDef, id::ColumnId, primitive::PrimitiveId},
	key::column::ColumnKey,
};
use reifydb_transaction::transaction::AsTransaction;

use crate::{CatalogStore, store::column::schema::primitive_column};

impl CatalogStore {
	pub(crate) fn find_column_by_name(
		rx: &mut impl AsTransaction,
		source: impl Into<PrimitiveId>,
		column_name: &str,
	) -> crate::Result<Option<ColumnDef>> {
		let mut txn = rx.as_transaction();
		let mut stream = txn.range(ColumnKey::full_scan(source), 1024)?;

		let mut found_id = None;
		while let Some(entry) = stream.next() {
			let multi = entry?;
			let row = multi.values;
			let column = ColumnId(primitive_column::SCHEMA.get_u64(&row, primitive_column::ID));
			let name = primitive_column::SCHEMA.get_utf8(&row, primitive_column::NAME);

			if name == column_name {
				found_id = Some(column);
				break;
			}
		}

		drop(stream);

		if let Some(id) = found_id {
			Ok(Some(Self::get_column(&mut txn, id)?))
		} else {
			Ok(None)
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::{ColumnId, TableId};
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

	use crate::{CatalogStore, test_utils::create_test_column};

	#[test]
	fn test_ok() {
		let mut txn = create_test_command_transaction();
		create_test_column(&mut txn, "col_1", TypeConstraint::unconstrained(Type::Int1), vec![]);
		create_test_column(&mut txn, "col_2", TypeConstraint::unconstrained(Type::Int2), vec![]);
		create_test_column(&mut txn, "col_3", TypeConstraint::unconstrained(Type::Int4), vec![]);

		let result = CatalogStore::find_column_by_name(&mut txn, TableId(1), "col_3").unwrap().unwrap();

		assert_eq!(result.id, ColumnId(8195));
		assert_eq!(result.name, "col_3");
		assert_eq!(result.constraint.get_type(), Type::Int4);
		assert_eq!(result.auto_increment, false);
	}

	#[test]
	fn test_not_found() {
		let mut txn = create_test_command_transaction();
		create_test_column(&mut txn, "col_1", TypeConstraint::unconstrained(Type::Int1), vec![]);

		let result = CatalogStore::find_column_by_name(&mut txn, TableId(1), "not_found").unwrap();

		assert!(result.is_none());
	}
}
