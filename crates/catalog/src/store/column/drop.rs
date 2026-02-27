// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{id::ColumnId, primitive::PrimitiveId},
	key::{
		column::ColumnKey, column_sequence::ColumnSequenceKey, columns::ColumnsKey, property::ColumnPropertyKey,
	},
};
use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::CatalogStore;

impl CatalogStore {
	pub(crate) fn drop_column(
		txn: &mut AdminTransaction,
		primitive: PrimitiveId,
		column_id: ColumnId,
	) -> crate::Result<()> {
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
		txn.remove(&ColumnSequenceKey::encoded(primitive, column_id))?;

		// Delete column definition
		txn.remove(&ColumnsKey::encoded(column_id))?;

		// Delete primitive-column link
		txn.remove(&ColumnKey::encoded(primitive, column_id))?;

		Ok(())
	}
}
