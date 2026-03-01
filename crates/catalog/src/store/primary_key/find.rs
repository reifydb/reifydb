// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		column::ColumnDef,
		id::{TableId, ViewId},
		key::PrimaryKeyDef,
		primitive::PrimitiveId,
	},
	key::primary_key::PrimaryKeyKey,
	return_internal_error,
};
use reifydb_transaction::transaction::Transaction;

use crate::{
	CatalogStore, Result,
	store::primary_key::schema::{primary_key, primary_key::deserialize_column_ids},
};

impl CatalogStore {
	pub(crate) fn find_primary_key(
		rx: &mut Transaction<'_>,
		primitive: impl Into<PrimitiveId>,
	) -> Result<Option<PrimaryKeyDef>> {
		let primitive_id = primitive.into();

		let primary_key_id = match primitive_id {
			PrimitiveId::Table(table_id) => match Self::get_table_pk_id(rx, table_id)? {
				Some(pk_id) => pk_id,
				None => return Ok(None),
			},
			PrimitiveId::View(view_id) => match Self::get_view_pk_id(rx, view_id)? {
				Some(pk_id) => pk_id,
				None => return Ok(None),
			},
			PrimitiveId::TableVirtual(_) => {
				// Virtual tables don't have primary keys
				return Ok(None);
			}
			PrimitiveId::RingBuffer(ringbuffer_id) => {
				match Self::get_ringbuffer_pk_id(rx, ringbuffer_id)? {
					Some(pk_id) => pk_id,
					None => return Ok(None),
				}
			}
			PrimitiveId::Dictionary(_) => {
				// Dictionaries don't have traditional primary keys
				return Ok(None);
			}
			PrimitiveId::Series(_) => {
				// Series use timestamp-based key ordering, no traditional primary keys
				return Ok(None);
			}
		};

		// Fetch the primary key details
		let primary_key_multi = match rx.get(&PrimaryKeyKey::encoded(primary_key_id))? {
			Some(multi) => multi,
			None => return_internal_error!(format!(
				"Primary key with ID {:?} referenced but not found",
				primary_key_id
			)),
		};

		// Deserialize column IDs
		let column_ids_blob = primary_key::SCHEMA.get_blob(&primary_key_multi.values, primary_key::COLUMN_IDS);
		let column_ids = deserialize_column_ids(&column_ids_blob);

		// Fetch full ColumnDef for each column ID
		let mut columns = Vec::new();
		for column_id in column_ids {
			let column_def = Self::get_column(rx, column_id)?;
			columns.push(ColumnDef {
				id: column_def.id,
				name: column_def.name,
				constraint: column_def.constraint,
				properties: column_def.properties,
				index: column_def.index,
				auto_increment: column_def.auto_increment,
				dictionary_id: None,
			});
		}

		Ok(Some(PrimaryKeyDef {
			id: primary_key_id,
			columns,
		}))
	}

	#[inline]
	pub(crate) fn find_table_primary_key(
		rx: &mut Transaction<'_>,
		table_id: TableId,
	) -> Result<Option<PrimaryKeyDef>> {
		Self::find_primary_key(rx, table_id)
	}

	#[inline]
	pub(crate) fn find_view_primary_key(
		rx: &mut Transaction<'_>,
		view_id: ViewId,
	) -> Result<Option<PrimaryKeyDef>> {
		Self::find_primary_key(rx, view_id)
	}
}
