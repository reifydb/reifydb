// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Type,
	interface::{
		EncodableKey, VersionedQueryTransaction, ViewColumnKey,
		ViewColumnsKey, ViewId,
	},
};

use crate::{
	Catalog,
	view_column::{
		ColumnDef, ColumnId, ColumnIndex,
		layout::{view_column, view_column_link},
	},
};

impl Catalog {
	pub fn get_view_column(
		rx: &mut impl VersionedQueryTransaction,
		column: ColumnId,
	) -> crate::Result<Option<ColumnDef>> {
		match rx.get(&ViewColumnsKey {
			column,
		}
		.encode())?
		{
			None => Ok(None),
			Some(versioned) => {
				let row = versioned.row;

				let id = ColumnId(
					view_column::LAYOUT
						.get_u64(&row, view_column::ID),
				);
				let name = view_column::LAYOUT
					.get_utf8(&row, view_column::NAME)
					.to_string();
				let value = Type::from_u8(
					view_column::LAYOUT.get_u8(
						&row,
						view_column::VALUE,
					),
				);
				let index = ColumnIndex(
					view_column::LAYOUT.get_u16(
						&row,
						view_column::INDEX,
					),
				);

				Ok(Some(ColumnDef {
					id,
					name,
					ty: value,
					index,
				}))
			}
		}
	}

	pub fn get_view_column_by_name(
		rx: &mut impl VersionedQueryTransaction,
		view: ViewId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<ColumnDef>> {
		let name = name.as_ref();

		let Some(id) = rx
			.range(ViewColumnKey::full_scan(view))?
			.find_map(|versioned| {
				let row = versioned.row;
				let column_name = view_column_link::LAYOUT
					.get_utf8(&row, view_column_link::NAME);
				if name == column_name {
					Some(ColumnId(
						view_column_link::LAYOUT
							.get_u64(
							&row,
							view_column_link::ID,
						),
					))
				} else {
					None
				}
			})
		else {
			return Ok(None);
		};

		Catalog::get_view_column(rx, id)
	}
}
