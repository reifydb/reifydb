// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{ColumnKey, QueryTransaction, SourceId};

use crate::{
	CatalogStore,
	store::column::{ColumnDef, ColumnId, layout::source_column},
};

impl CatalogStore {
	pub fn find_column_by_name(
		rx: &mut impl QueryTransaction,
		source: impl Into<SourceId>,
		column_name: &str,
	) -> crate::Result<Option<ColumnDef>> {
		let maybe_id = rx.range(ColumnKey::full_scan(source))?.find_map(|multi| {
			let row = multi.values;
			let column = ColumnId(source_column::LAYOUT.get_u64(&row, source_column::ID));
			let name = source_column::LAYOUT.get_utf8(&row, source_column::NAME);

			if name == column_name {
				Some(column)
			} else {
				None
			}
		});

		if let Some(id) = maybe_id {
			Ok(Some(Self::get_column(rx, id)?))
		} else {
			Ok(None)
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{ColumnId, TableId};
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::{Type, TypeConstraint};

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
