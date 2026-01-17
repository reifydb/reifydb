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
};
use reifydb_transaction::standard::IntoStandardTransaction;
use reifydb_type::return_internal_error;

use crate::{
	CatalogStore,
	store::primary_key::schema::{primary_key, primary_key::deserialize_column_ids},
};

impl CatalogStore {
	pub(crate) fn find_primary_key(
		rx: &mut impl IntoStandardTransaction,
		primitive: impl Into<PrimitiveId>,
	) -> crate::Result<Option<PrimaryKeyDef>> {
		let primitive_id = primitive.into();
		let mut txn = rx.into_standard_transaction();

		let primary_key_id = match primitive_id {
			PrimitiveId::Table(table_id) => match Self::get_table_pk_id(&mut txn, table_id)? {
				Some(pk_id) => pk_id,
				None => return Ok(None),
			},
			PrimitiveId::View(view_id) => match Self::get_view_pk_id(&mut txn, view_id)? {
				Some(pk_id) => pk_id,
				None => return Ok(None),
			},
			PrimitiveId::Flow(_) => {
				// Flows don't have primary keys
				return Ok(None);
			}
			PrimitiveId::TableVirtual(_) => {
				// Virtual tables don't have primary keys
				return Ok(None);
			}
			PrimitiveId::RingBuffer(ringbuffer_id) => {
				match Self::get_ringbuffer_pk_id(&mut txn, ringbuffer_id)? {
					Some(pk_id) => pk_id,
					None => return Ok(None),
				}
			}
			PrimitiveId::Dictionary(_) => {
				// Dictionaries don't have traditional primary keys
				return Ok(None);
			}
		};

		// Fetch the primary key details
		let primary_key_multi = match txn.get(&PrimaryKeyKey::encoded(primary_key_id))? {
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
			let column_def = Self::get_column(&mut txn, column_id)?;
			columns.push(ColumnDef {
				id: column_def.id,
				name: column_def.name,
				constraint: column_def.constraint,
				policies: column_def.policies,
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
		rx: &mut impl IntoStandardTransaction,
		table_id: TableId,
	) -> crate::Result<Option<PrimaryKeyDef>> {
		Self::find_primary_key(rx, table_id)
	}

	#[inline]
	pub(crate) fn find_view_primary_key(
		rx: &mut impl IntoStandardTransaction,
		view_id: ViewId,
	) -> crate::Result<Option<PrimaryKeyDef>> {
		Self::find_primary_key(rx, view_id)
	}
}
