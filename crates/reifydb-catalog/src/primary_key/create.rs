// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use primary_key::LAYOUT;
use reifydb_core::{
	diagnostic::catalog::table_not_found,
	interface::{
		ColumnId, CommandTransaction, Key, PrimaryKeyId, PrimaryKeyKey,
		StoreId, TableKey, ViewKey,
	},
	return_error,
};

use crate::{
	CatalogStore,
	primary_key::layout::{primary_key, primary_key::serialize_column_ids},
	sequence::SystemSequence,
};

pub struct PrimaryKeyToCreate {
	pub store: StoreId,
	pub column_ids: Vec<ColumnId>,
}

impl CatalogStore {
	pub fn create_primary_key(
		txn: &mut impl CommandTransaction,
		primary_key_to_create: PrimaryKeyToCreate,
	) -> crate::Result<PrimaryKeyId> {
		let id = SystemSequence::next_primary_key_id(txn)?;

		// Create primary key row
		let mut row = LAYOUT.allocate_row();
		LAYOUT.set_u64(&mut row, primary_key::ID, id.0);
		LAYOUT.set_u64(
			&mut row,
			primary_key::STORE,
			primary_key_to_create.store.as_u64(),
		);
		LAYOUT.set_blob(
			&mut row,
			primary_key::COLUMN_IDS,
			&serialize_column_ids(
				&primary_key_to_create.column_ids,
			),
		);

		// Store the primary key
		txn.set(
			&Key::PrimaryKey(PrimaryKeyKey {
				primary_key: id,
			})
			.encode(),
			row,
		)?;

		// Update the table or view to reference this primary key
		match primary_key_to_create.store {
			StoreId::Table(table_id) => {
				if let Some(versioned) =
					txn.get(&Key::Table(TableKey {
						table: table_id,
					})
					.encode())?
				{
					let mut updated_row =
						versioned.row.clone();
					crate::table::layout::table::LAYOUT.set_u64(&mut updated_row, crate::table::layout::table::PRIMARY_KEY, id.0);
					txn.set(
						&Key::Table(TableKey {
							table: table_id,
						})
						.encode(),
						updated_row,
					)?;
				} else {
					return_error!(table_not_found(
						None,
						"unknown",
						&format!(
							"table_{}",
							table_id.0
						)
					));
				}
			}
			StoreId::View(view_id) => {
				if let Some(versioned) =
					txn.get(&Key::View(ViewKey {
						view: view_id,
					})
					.encode())?
				{
					let mut updated_row =
						versioned.row.clone();
					crate::view::layout::view::LAYOUT.set_u64(&mut updated_row, crate::view::layout::view::PRIMARY_KEY, id.0);
					txn.set(
						&Key::View(ViewKey {
							view: view_id,
						})
						.encode(),
						updated_row,
					)?;
				} else {
					return_error!(table_not_found(
						None,
						"unknown",
						&format!("view_{}", view_id.0)
					));
				}
			}
		}

		Ok(id)
	}
}
