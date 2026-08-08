// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{id::ColumnId, object::ObjectId},
	key::{column::ColumnKey, columns::ColumnsKey},
};
use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::{
	CatalogStore, Result,
	store::column::shape::{column, object_column},
};

impl CatalogStore {
	pub(crate) fn rename_column(
		txn: &mut AdminTransaction,
		object: ObjectId,
		column_id: ColumnId,
		new_name: &str,
	) -> Result<()> {
		if let Some(multi) = txn.get(&ColumnsKey::encoded(column_id))? {
			let old = multi.bytes;
			let mut row = column::allocate();
			column::set_id(&mut row, column::get_id(&old));
			column::set_object(&mut row, column::get_object(&old));
			column::set_name(&mut row, new_name);
			column::set_value(&mut row, column::get_value(&old));
			column::set_index(&mut row, column::get_index(&old));
			column::set_auto_increment(&mut row, column::get_auto_increment(&old));
			column::set_constraint(&mut row, &column::get_constraint(&old));
			column::set_dictionary_id(&mut row, column::get_dictionary_id(&old));
			txn.set(&ColumnsKey::encoded(column_id), row.freeze())?;
		}

		if let Some(multi) = txn.get(&ColumnKey::encoded(object, column_id))? {
			let old = multi.bytes;
			let mut row = object_column::allocate();
			object_column::set_id(&mut row, object_column::get_id(&old));
			object_column::set_name(&mut row, new_name);
			object_column::set_index(&mut row, object_column::get_index(&old));
			txn.set(&ColumnKey::encoded(object, column_id), row.freeze())?;
		}

		Ok(())
	}
}
