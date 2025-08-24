// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	view_column::{layout::view_column, ColumnDef, ColumnId, ColumnIndex},
	CatalogStore,
};
use reifydb_core::interface::QueryTransaction;
use reifydb_core::{
	interface::{EncodableKey, ViewColumnsKey}, internal_error,
	Error,
	Type,
};

impl CatalogStore {
	pub fn get_view_column(
		rx: &mut impl QueryTransaction,
		column: ColumnId,
	) -> crate::Result<ColumnDef> {
		let versioned = rx
			.get(&ViewColumnsKey { column }.encode())?
			.ok_or_else(|| {
				Error(internal_error!(
					"View column with ID {:?} not found in Self:: This indicates a critical catalog inconsistency.",
					column
				))
			})?;

		let row = versioned.row;
		let id = ColumnId(
			view_column::LAYOUT.get_u64(&row, view_column::ID),
		);
		let name = view_column::LAYOUT
			.get_utf8(&row, view_column::NAME)
			.to_string();
		let ty = Type::from_u8(
			view_column::LAYOUT.get_u8(&row, view_column::VALUE),
		);
		let index = ColumnIndex(
			view_column::LAYOUT.get_u16(&row, view_column::INDEX),
		);

		Ok(ColumnDef {
			id,
			name,
			ty,
			index,
		})
	}
}
