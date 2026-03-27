// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{id::ColumnId, schema::SchemaId},
	key::{
		column::ColumnKey, column_sequence::ColumnSequenceKey, columns::ColumnsKey, property::ColumnPropertyKey,
	},
};
use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn drop_column(txn: &mut AdminTransaction, object: SchemaId, column_id: ColumnId) -> Result<()> {
		// Delete column policies
		let policy_range = ColumnPropertyKey::full_scan(column_id);
		let mut policy_stream = txn.range(policy_range, 1024)?;
		let mut policy_keys = Vec::new();
		while let Some(entry) = policy_stream.next() {
			policy_keys.push(entry?.key.clone());
		}
		drop(policy_stream);
		for pk in policy_keys {
			txn.remove(&pk)?;
		}

		// Delete column sequence
		txn.remove(&ColumnSequenceKey::encoded(object, column_id))?;

		// Delete column definition
		txn.remove(&ColumnsKey::encoded(column_id))?;

		// Delete object-column link
		txn.remove(&ColumnKey::encoded(object, column_id))?;

		Ok(())
	}
}
