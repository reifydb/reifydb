// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{
	interface::catalog::{
		change::CatalogTrackPrimaryKeyChangeOperations,
		id::{ColumnId, PrimaryKeyId},
		key::PrimaryKey,
		object::ObjectId,
		storage::StorageId,
	},
	key::{
		column::ColumnKey, column_sequence::ColumnSequenceKey, columns::ColumnsKey, primary_key::PrimaryKeyKey,
		property::ColumnPropertyKey, row_sequence::RowSequenceKey,
	},
};
use reifydb_transaction::{multi::RangeScope, transaction::admin::AdminTransaction};

use crate::{Result, store::column::shape::object_column};

pub(crate) fn drop_object_metadata(
	txn: &mut AdminTransaction,
	storage: StorageId,
	pk_id: Option<PrimaryKeyId>,
) -> Result<()> {
	let range = ColumnKey::full_scan(storage);
	let mut stream = txn.range(range, RangeScope::All, 1024)?;
	let mut col_entries = Vec::new();
	for entry in stream.by_ref() {
		let entry = entry?;
		let col_id = object_column::get_id(EncodedCatalogRow::view(&entry.bytes));
		col_entries.push((entry.key.clone(), ColumnId(col_id)));
	}
	drop(stream);

	for (col_key, col_id) in &col_entries {
		let policy_range = ColumnPropertyKey::full_scan(*col_id);
		let mut policy_stream = txn.range(policy_range, RangeScope::All, 1024)?;
		let mut policy_keys = Vec::new();
		for entry in policy_stream.by_ref() {
			policy_keys.push(entry?.key.clone());
		}
		drop(policy_stream);
		for pk in policy_keys {
			txn.remove(&pk)?;
		}

		txn.remove(&ColumnSequenceKey::encoded(storage, *col_id))?;

		txn.remove(&ColumnsKey::encoded(*col_id))?;

		txn.remove(col_key)?;
	}

	if let Some(pk_id) = pk_id {
		txn.track_primary_key_deleted(
			ObjectId::from(storage),
			PrimaryKey {
				id: pk_id,
				columns: Vec::new(),
			},
		)?;
		txn.remove(&PrimaryKeyKey::encoded(pk_id))?;
	}

	txn.remove(&RowSequenceKey::encoded(storage))?;

	Ok(())
}
