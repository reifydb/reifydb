// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{ColumnDef, Key, PrimaryKeyDef, PrimaryKeyKey, QueryTransaction, SourceId, TableId, ViewId},
	return_internal_error,
};

use crate::{
	CatalogStore,
	primary_key::layout::{primary_key, primary_key::deserialize_column_ids},
};

impl CatalogStore {
	pub fn find_primary_key(
		rx: &mut impl QueryTransaction,
		source: impl Into<SourceId>,
	) -> crate::Result<Option<PrimaryKeyDef>> {
		let source_id = source.into();

		// Get the primary key ID for the table or view
		// Virtual tables and ring buffers don't have primary keys
		// stored separately
		let primary_key_id = match source_id {
			SourceId::Table(table_id) => match Self::get_table_pk_id(rx, table_id)? {
				Some(pk_id) => pk_id,
				None => return Ok(None),
			},
			SourceId::View(view_id) => match Self::get_view_pk_id(rx, view_id)? {
				Some(pk_id) => pk_id,
				None => return Ok(None),
			},
			SourceId::TableVirtual(_) => {
				// Virtual tables don't have primary keys
				return Ok(None);
			}
			SourceId::RingBuffer(ring_buffer_id) => {
				match Self::get_ring_buffer_pk_id(rx, ring_buffer_id)? {
					Some(pk_id) => pk_id,
					None => return Ok(None),
				}
			}
		};

		// Fetch the primary key details
		let primary_key_multi = match rx.get(&Key::PrimaryKey(PrimaryKeyKey {
			primary_key: primary_key_id,
		})
		.encode())?
		{
			Some(multi) => multi,
			None => return_internal_error!(format!(
				"Primary key with ID {:?} referenced but not found",
				primary_key_id
			)),
		};

		// Deserialize column IDs
		let column_ids_blob = primary_key::LAYOUT.get_blob(&primary_key_multi.values, primary_key::COLUMN_IDS);
		let column_ids = deserialize_column_ids(&column_ids_blob);

		// Fetch full ColumnDef for each column ID
		let mut columns = Vec::new();
		for column_id in column_ids {
			let column_def = Self::get_column(rx, column_id)?;
			columns.push(ColumnDef {
				id: column_def.id,
				name: column_def.name,
				constraint: column_def.constraint,
				policies: column_def.policies,
				index: column_def.index,
				auto_increment: column_def.auto_increment,
			});
		}

		Ok(Some(PrimaryKeyDef {
			id: primary_key_id,
			columns,
		}))
	}

	#[inline]
	pub fn find_table_primary_key(
		rx: &mut impl QueryTransaction,
		table_id: TableId,
	) -> crate::Result<Option<PrimaryKeyDef>> {
		Self::find_primary_key(rx, table_id)
	}

	#[inline]
	pub fn find_view_primary_key(
		rx: &mut impl QueryTransaction,
		view_id: ViewId,
	) -> crate::Result<Option<PrimaryKeyDef>> {
		Self::find_primary_key(rx, view_id)
	}
}
