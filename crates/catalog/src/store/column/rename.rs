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
			let old = multi.row;
			let mut row = column::SHAPE.allocate();
			column::SHAPE.set::<u64>(&mut row, column::ID, column::SHAPE.get::<u64>(&old, column::ID));
			column::SHAPE.set::<u64>(
				&mut row,
				column::OBJECT,
				column::SHAPE.get::<u64>(&old, column::OBJECT),
			);
			column::SHAPE.set_utf8(&mut row, column::NAME, new_name);
			column::SHAPE.set::<u8>(&mut row, column::VALUE, column::SHAPE.get::<u8>(&old, column::VALUE));
			column::SHAPE.set::<u8>(&mut row, column::INDEX, column::SHAPE.get::<u8>(&old, column::INDEX));
			column::SHAPE.set::<bool>(
				&mut row,
				column::AUTO_INCREMENT,
				column::SHAPE.get::<bool>(&old, column::AUTO_INCREMENT),
			);
			column::SHAPE.set_blob(
				&mut row,
				column::CONSTRAINT,
				&column::SHAPE.get_blob(&old, column::CONSTRAINT),
			);
			column::SHAPE.set::<u64>(
				&mut row,
				column::DICTIONARY_ID,
				column::SHAPE.get::<u64>(&old, column::DICTIONARY_ID),
			);
			txn.set(&ColumnsKey::encoded(column_id), row.freeze())?;
		}

		if let Some(multi) = txn.get(&ColumnKey::encoded(object, column_id))? {
			let old = multi.row;
			let mut row = object_column::SHAPE.allocate();
			object_column::SHAPE.set::<u64>(
				&mut row,
				object_column::ID,
				object_column::SHAPE.get::<u64>(&old, object_column::ID),
			);
			object_column::SHAPE.set_utf8(&mut row, object_column::NAME, new_name);
			object_column::SHAPE.set::<u8>(
				&mut row,
				object_column::INDEX,
				object_column::SHAPE.get::<u8>(&old, object_column::INDEX),
			);
			txn.set(&ColumnKey::encoded(object, column_id), row.freeze())?;
		}

		Ok(())
	}
}
