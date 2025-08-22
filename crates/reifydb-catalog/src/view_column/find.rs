// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	UnderlyingQueryTransaction, VersionedQueryTransaction, ViewColumnKey,
	ViewId,
};

use crate::{
	view_column::{layout::view_column_link, ColumnDef, ColumnId},
	Catalog,
};

impl Catalog {
	pub fn find_view_column_by_name(
		&self,
		rx: &mut impl UnderlyingQueryTransaction,
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

		Ok(Some(self.get_view_column(rx, id)?))
	}
}
