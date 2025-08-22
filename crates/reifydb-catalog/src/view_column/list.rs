// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	QueryTransaction, VersionedQueryTransaction, ViewColumnKey,
	ViewId,
};

use crate::{
	view_column::{layout::view_column_link, ColumnDef, ColumnId},
	Catalog,
};

impl Catalog {
	pub fn list_view_columns(
		&self,
		rx: &mut impl QueryTransaction,
		view: ViewId,
	) -> crate::Result<Vec<ColumnDef>> {
		let mut result = vec![];

		let ids = rx
			.range(ViewColumnKey::full_scan(view))?
			.map(|versioned| {
				let row = versioned.row;
				ColumnId(
					view_column_link::LAYOUT.get_u64(
						&row,
						view_column_link::ID,
					),
				)
			})
			.collect::<Vec<_>>();

		for id in ids {
			result.push(self.get_view_column(rx, id)?);
		}

		result.sort_by_key(|c| c.index);

		Ok(result)
	}
}
