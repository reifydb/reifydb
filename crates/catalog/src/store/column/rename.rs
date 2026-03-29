// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{id::ColumnId, shape::ShapeId},
	key::{column::ColumnKey, columns::ColumnsKey},
};
use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::{
	CatalogStore, Result,
	store::column::shape::{column, primitive_column},
};

impl CatalogStore {
	pub(crate) fn rename_column(
		txn: &mut AdminTransaction,
		shape: ShapeId,
		column_id: ColumnId,
		new_name: &str,
	) -> Result<()> {
		// Update column definition (ColumnsKey)
		// We must rebuild the row since set_utf8 cannot overwrite existing values
		if let Some(multi) = txn.get(&ColumnsKey::encoded(column_id))? {
			let old = multi.row;
			let mut row = column::SHAPE.allocate();
			column::SHAPE.set_u64(&mut row, column::ID, column::SHAPE.get_u64(&old, column::ID));
			column::SHAPE.set_u64(
				&mut row,
				column::PRIMITIVE,
				column::SHAPE.get_u64(&old, column::PRIMITIVE),
			);
			column::SHAPE.set_utf8(&mut row, column::NAME, new_name);
			column::SHAPE.set_u8(&mut row, column::VALUE, column::SHAPE.get_u8(&old, column::VALUE));
			column::SHAPE.set_u8(&mut row, column::INDEX, column::SHAPE.get_u8(&old, column::INDEX));
			column::SHAPE.set_bool(
				&mut row,
				column::AUTO_INCREMENT,
				column::SHAPE.get_bool(&old, column::AUTO_INCREMENT),
			);
			column::SHAPE.set_blob(
				&mut row,
				column::CONSTRAINT,
				&column::SHAPE.get_blob(&old, column::CONSTRAINT),
			);
			column::SHAPE.set_u64(
				&mut row,
				column::DICTIONARY_ID,
				column::SHAPE.get_u64(&old, column::DICTIONARY_ID),
			);
			txn.set(&ColumnsKey::encoded(column_id), row)?;
		}

		// Update shape-column link (ColumnKey)
		if let Some(multi) = txn.get(&ColumnKey::encoded(shape, column_id))? {
			let old = multi.row;
			let mut row = primitive_column::SHAPE.allocate();
			primitive_column::SHAPE.set_u64(
				&mut row,
				primitive_column::ID,
				primitive_column::SHAPE.get_u64(&old, primitive_column::ID),
			);
			primitive_column::SHAPE.set_utf8(&mut row, primitive_column::NAME, new_name);
			primitive_column::SHAPE.set_u8(
				&mut row,
				primitive_column::INDEX,
				primitive_column::SHAPE.get_u8(&old, primitive_column::INDEX),
			);
			txn.set(&ColumnKey::encoded(shape, column_id), row)?;
		}

		Ok(())
	}
}
