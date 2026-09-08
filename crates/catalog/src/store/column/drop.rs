// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{id::ColumnId, object::ObjectId},
	key::{
		any::TaggedKey,
		catalog::ColumnPropertyKey,
		column::{ColumnKey, ColumnSequenceKey, ColumnsKey},
	},
	return_internal_error,
};
use reifydb_transaction::{multi::RangeScope, transaction::admin::AdminTransaction};

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn drop_column(txn: &mut AdminTransaction, object: ObjectId, column_id: ColumnId) -> Result<()> {
		let policy_range = ColumnPropertyKey::full_scan(column_id);
		let mut policy_stream = txn.range(policy_range, RangeScope::All, 1024)?;
		let mut policy_keys = Vec::new();
		for entry in policy_stream.by_ref() {
			let TaggedKey::ColumnProperty(key) = entry?.key else {
				return_internal_error!(
					"column property scan yielded a key that is not a ColumnPropertyKey"
				);
			};
			policy_keys.push(key);
		}
		drop(policy_stream);
		for pk in policy_keys {
			txn.remove(&pk)?;
		}

		txn.remove(&ColumnSequenceKey::new(object, column_id))?;

		txn.remove(&ColumnsKey::new(column_id))?;

		txn.remove(&ColumnKey::new(object, column_id))?;

		Ok(())
	}
}
