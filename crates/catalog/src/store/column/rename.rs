// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{id::ColumnId, primitive::PrimitiveId},
	key::{column::ColumnKey, columns::ColumnsKey},
};
use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::{
	CatalogStore, Result,
	store::column::schema::{column, primitive_column},
};

impl CatalogStore {
	pub(crate) fn rename_column(
		txn: &mut AdminTransaction,
		primitive: PrimitiveId,
		column_id: ColumnId,
		new_name: &str,
	) -> Result<()> {
		// Update column definition (ColumnsKey)
		// We must rebuild the row since set_utf8 cannot overwrite existing values
		if let Some(multi) = txn.get(&ColumnsKey::encoded(column_id))? {
			let old = multi.values;
			let mut row = column::SCHEMA.allocate();
			column::SCHEMA.set_u64(&mut row, column::ID, column::SCHEMA.get_u64(&old, column::ID));
			column::SCHEMA.set_u64(
				&mut row,
				column::PRIMITIVE,
				column::SCHEMA.get_u64(&old, column::PRIMITIVE),
			);
			column::SCHEMA.set_utf8(&mut row, column::NAME, new_name);
			column::SCHEMA.set_u8(&mut row, column::VALUE, column::SCHEMA.get_u8(&old, column::VALUE));
			column::SCHEMA.set_u8(&mut row, column::INDEX, column::SCHEMA.get_u8(&old, column::INDEX));
			column::SCHEMA.set_bool(
				&mut row,
				column::AUTO_INCREMENT,
				column::SCHEMA.get_bool(&old, column::AUTO_INCREMENT),
			);
			column::SCHEMA.set_blob(
				&mut row,
				column::CONSTRAINT,
				&column::SCHEMA.get_blob(&old, column::CONSTRAINT),
			);
			column::SCHEMA.set_u64(
				&mut row,
				column::DICTIONARY_ID,
				column::SCHEMA.get_u64(&old, column::DICTIONARY_ID),
			);
			txn.set(&ColumnsKey::encoded(column_id), row)?;
		}

		// Update primitive-column link (ColumnKey)
		if let Some(multi) = txn.get(&ColumnKey::encoded(primitive, column_id))? {
			let old = multi.values;
			let mut row = primitive_column::SCHEMA.allocate();
			primitive_column::SCHEMA.set_u64(
				&mut row,
				primitive_column::ID,
				primitive_column::SCHEMA.get_u64(&old, primitive_column::ID),
			);
			primitive_column::SCHEMA.set_utf8(&mut row, primitive_column::NAME, new_name);
			primitive_column::SCHEMA.set_u8(
				&mut row,
				primitive_column::INDEX,
				primitive_column::SCHEMA.get_u8(&old, primitive_column::INDEX),
			);
			txn.set(&ColumnKey::encoded(primitive, column_id), row)?;
		}

		Ok(())
	}
}
