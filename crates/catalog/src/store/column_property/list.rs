// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{
	interface::catalog::{
		id::{ColumnId, ColumnPropertyId},
		property::{ColumnProperty, ColumnPropertyKind},
	},
	key::property::ColumnPropertyKey,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use crate::{CatalogStore, Result, store::column_property::shape::column_property};

impl CatalogStore {
	pub(crate) fn list_column_properties(
		rx: &mut Transaction<'_>,
		column: ColumnId,
	) -> Result<Vec<ColumnProperty>> {
		let stream = rx.range(ColumnPropertyKey::full_scan(column), RangeScope::All, 1024)?;
		let mut result = Vec::new();

		for entry in stream {
			let multi = entry?;
			let bytes = EncodedCatalogRow::try_from(multi.bytes)?;
			let id = ColumnPropertyId(column_property::get_id(&bytes));
			let column = ColumnId(column_property::get_column(&bytes));

			let property = ColumnPropertyKind::from_u8(
				column_property::get_policy(&bytes),
				column_property::get_value(&bytes),
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

		let columns = CatalogStore::list_columns_all(rx)?;

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
		property::{ColumnPropertyKind, ColumnSaturationStrategy},
	};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::value::{constraint::TypeConstraint, value_type::ValueType};

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
				object_name: "test_table".to_string(),
				column: "with_policy".to_string(),
				constraint: TypeConstraint::unconstrained(ValueType::Int2),
				properties: vec![ColumnPropertyKind::Saturation(ColumnSaturationStrategy::None)],
				index: ColumnIndex(0),
				auto_increment: false,
				dictionary_id: None,
			},
		)
		.unwrap();

		let column = CatalogStore::get_column(&mut Transaction::Admin(&mut txn), ColumnId(16385)).unwrap();

		let properties =
			CatalogStore::list_column_properties(&mut Transaction::Admin(&mut txn), column.id).unwrap();

		assert_eq!(properties.len(), 1);
		assert_eq!(properties[0].column, column.id);
		assert!(matches!(
			properties[0].property,
			ColumnPropertyKind::Saturation(ColumnSaturationStrategy::None)
		));
	}
}
