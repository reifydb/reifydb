// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	OwnedFragment, Type,
	diagnostic::catalog::view_column_already_exists,
	interface::{
		ActiveCommandTransaction, EncodableKey, Key, Transaction,
		VersionedCommandTransaction, ViewColumnKey, ViewColumnsKey,
		ViewId,
	},
	return_error,
};

use crate::{
	Catalog,
	sequence::SystemSequence,
	view_column::{
		ColumnDef, ColumnIndex,
		layout::{view_column, view_column_link},
	},
};

pub struct ViewColumnToCreate<'a> {
	pub fragment: Option<OwnedFragment>,
	pub schema_name: &'a str,
	pub view: ViewId,
	pub view_name: &'a str,
	pub column: String,
	pub value: Type,
	pub if_not_exists: bool,
	pub index: ColumnIndex,
}

impl Catalog {
	pub(crate) fn create_view_column<T: Transaction>(
		txn: &mut ActiveCommandTransaction<T>,
		view: ViewId,
		column_to_create: ViewColumnToCreate,
	) -> crate::Result<ColumnDef> {
		if let Some(column) = Catalog::find_view_column_by_name(
			txn,
			view,
			&column_to_create.column,
		)? {
			if column_to_create.if_not_exists {
				return Ok(column);
			}

			return_error!(view_column_already_exists(
				None::<OwnedFragment>,
				column_to_create.schema_name,
				column_to_create.view_name,
				&column.name,
			));
		}

		let id = SystemSequence::next_view_column_id(txn)?;

		let mut row = view_column::LAYOUT.allocate_row();
		view_column::LAYOUT.set_u64(&mut row, view_column::ID, id);
		view_column::LAYOUT.set_u64(
			&mut row,
			view_column::VIEW,
			view.0,
		);
		view_column::LAYOUT.set_utf8(
			&mut row,
			view_column::NAME,
			&column_to_create.column,
		);
		view_column::LAYOUT.set_u8(
			&mut row,
			view_column::VALUE,
			column_to_create.value.to_u8(),
		);
		view_column::LAYOUT.set_u16(
			&mut row,
			view_column::INDEX,
			column_to_create.index,
		);

		txn.set(
			&Key::ViewColumns(ViewColumnsKey {
				column: id,
			})
			.encode(),
			row,
		)?;

		// Use ViewColumnKey for view-column relationship
		let mut row = view_column_link::LAYOUT.allocate_row();
		view_column_link::LAYOUT.set_u64(
			&mut row,
			view_column_link::ID,
			id,
		);
		view_column_link::LAYOUT.set_utf8(
			&mut row,
			view_column_link::NAME,
			&column_to_create.column,
		);
		view_column_link::LAYOUT.set_u16(
			&mut row,
			view_column_link::INDEX,
			column_to_create.index,
		);
		txn.set(
			&ViewColumnKey {
				view,
				column: id,
			}
			.encode(),
			row,
		)?;

		Ok(ColumnDef {
			id,
			name: column_to_create.column,
			ty: column_to_create.value,
			index: column_to_create.index,
		})
	}
}
