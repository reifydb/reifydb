// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{QueryTransaction, ViewColumnKey, ViewId};

use crate::{
	CatalogStore,
	view_column::{ColumnDef, ColumnId, layout::view_column_link},
};

impl CatalogStore {
	pub fn list_view_columns(
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
			result.push(Self::get_view_column(rx, id)?);
		}

		result.sort_by_key(|c| c.index);

		Ok(result)
	}
}
