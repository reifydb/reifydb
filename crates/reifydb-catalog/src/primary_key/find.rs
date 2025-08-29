// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use layout::view;
use reifydb_core::{
	interface::{
		ColumnDef, Key, PrimaryKeyId, PrimaryKeyKey, QueryTransaction,
		StoreId, TableId, TableKey, TablePrimaryKeyDef, ViewId,
		ViewKey,
	},
	return_internal_error,
};

use crate::{
	CatalogStore,
	primary_key::layout::{
		primary_key, primary_key::deserialize_column_ids,
	},
	table::layout::table,
	view::{layout, layout::view::PRIMARY_KEY},
};

impl CatalogStore {
	pub fn find_primary_key(
		rx: &mut impl QueryTransaction,
		store: impl Into<StoreId>,
	) -> crate::Result<Option<TablePrimaryKeyDef>> {
		let store_id = store.into();

		let primary_key_id = match store_id {
			StoreId::Table(table_id) => {
				let versioned =
					match rx.get(&Key::Table(TableKey {
						table: table_id,
					})
					.encode())?
					{
						Some(v) => v,
						None => return Ok(None),
					};
				table::LAYOUT.get_u64(
					&versioned.row,
					table::PRIMARY_KEY,
				)
			}
			StoreId::View(view_id) => {
				let versioned =
					match rx.get(&Key::View(ViewKey {
						view: view_id,
					})
					.encode())?
					{
						Some(v) => v,
						None => return Ok(None),
					};
				view::LAYOUT
					.get_u64(&versioned.row, PRIMARY_KEY)
			}
		};

		if primary_key_id == 0 {
			return Ok(None); // No primary key
		}

		// Fetch the primary key details
		let primary_key_versioned =
			match rx.get(&Key::PrimaryKey(PrimaryKeyKey {
				primary_key: PrimaryKeyId(primary_key_id),
			})
			.encode())?
			{
				Some(versioned) => versioned,
				None => return_internal_error!(format!(
					"Primary key with ID {} referenced but not found",
					primary_key_id
				)),
			};

		// Deserialize column IDs
		let column_ids_blob = primary_key::LAYOUT.get_blob(
			&primary_key_versioned.row,
			primary_key::COLUMN_IDS,
		);
		let column_ids = deserialize_column_ids(&column_ids_blob);

		// Fetch full ColumnDef for each column ID
		let mut columns = Vec::new();
		for column_id in column_ids {
			let column_def = Self::get_column(rx, column_id)?;
			columns.push(ColumnDef {
				id: column_def.id,
				name: column_def.name,
				ty: column_def.ty,
				policies: column_def.policies,
				index: column_def.index,
				auto_increment: column_def.auto_increment,
			});
		}

		Ok(Some(TablePrimaryKeyDef {
			id: PrimaryKeyId(primary_key_id),
			columns,
		}))
	}

	// Convenience methods for backward compatibility
	pub fn find_table_primary_key(
		rx: &mut impl QueryTransaction,
		table_id: TableId,
	) -> crate::Result<Option<TablePrimaryKeyDef>> {
		Self::find_primary_key(rx, table_id)
	}

	pub fn find_view_primary_key(
		rx: &mut impl QueryTransaction,
		view_id: ViewId,
	) -> crate::Result<Option<TablePrimaryKeyDef>> {
		Self::find_primary_key(rx, view_id)
	}
}
