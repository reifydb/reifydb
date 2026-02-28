// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		id::{ColumnId, ColumnPropertyId},
		property::{ColumnProperty, ColumnPropertyKind},
	},
	key::property::ColumnPropertyKey,
};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result, store::column_property::schema::column_property};

impl CatalogStore {
	pub(crate) fn list_column_properties(
		rx: &mut Transaction<'_>,
		column: ColumnId,
	) -> Result<Vec<ColumnProperty>> {
		let mut stream = rx.range(ColumnPropertyKey::full_scan(column), 1024)?;
		let mut result = Vec::new();

		while let Some(entry) = stream.next() {
			let multi = entry?;
			let row = multi.values;
			let id = ColumnPropertyId(column_property::SCHEMA.get_u64(&row, column_property::ID));
			let column = ColumnId(column_property::SCHEMA.get_u64(&row, column_property::COLUMN));

			let property = ColumnPropertyKind::from_u8(
				column_property::SCHEMA.get_u8(&row, column_property::POLICY),
				column_property::SCHEMA.get_u8(&row, column_property::VALUE),
			);

			result.push(ColumnProperty {
				id,
				column,
				property,
			});
		}

		Ok(result)
	}

	pub(crate) fn list_column_properties_all(rx: &mut Transaction<'_>) -> Result<Vec<ColumnProperty>> {
		let mut result = Vec::new();

		// Get all columns from tables and views
		let columns = CatalogStore::list_columns_all(rx)?;

		// For each column, get its policies
		for info in columns {
			let policies = CatalogStore::list_column_properties(rx, info.column.id)?;
			result.extend(policies);
		}

		Ok(result)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{
		column::ColumnIndex,
		id::{ColumnId, TableId},
		property::{ColumnPropertyKind, ColumnSaturationPolicy},
	};
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

	use crate::{CatalogStore, store::column::create::ColumnToCreate, test_utils::ensure_test_table};

	#[test]
	fn test_ok() {
		let mut txn = create_test_admin_transaction();
		ensure_test_table(&mut txn);

		CatalogStore::create_column(
			&mut txn,
			TableId(1),
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace".to_string(),
				primitive_name: "test_table".to_string(),
				column: "with_policy".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Int2),
				properties: vec![ColumnPropertyKind::Saturation(ColumnSaturationPolicy::None)],
				index: ColumnIndex(0),
				auto_increment: false,
				dictionary_id: None,
			},
		)
		.unwrap();

		let column = CatalogStore::get_column(&mut Transaction::Admin(&mut txn), ColumnId(8193)).unwrap();

		let properties =
			CatalogStore::list_column_properties(&mut Transaction::Admin(&mut txn), column.id).unwrap();

		assert_eq!(properties.len(), 1);
		assert_eq!(properties[0].column, column.id);
		assert!(matches!(properties[0].property, ColumnPropertyKind::Saturation(ColumnSaturationPolicy::None)));
	}
}
