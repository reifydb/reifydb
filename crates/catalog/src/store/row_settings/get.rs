// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::storage::StorageId, internal, row::RowSettings};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::error::Error;

use crate::{CatalogStore, Result};

impl CatalogStore {
	#[allow(dead_code)]
	pub fn get_row_settings(rx: &mut Transaction<'_>, storage: StorageId) -> Result<RowSettings> {
		Self::find_row_settings(rx, storage)?.ok_or_else(|| {
			Error(Box::new(internal!(
				"row settings for storage {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				storage
			)))
		})
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::{id::TableId, storage::StorageId},
		row::{RowSettings, Ttl},
	};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::value::duration::Duration;

	use super::*;
	use crate::store::row_settings::create::create_row_settings;

	#[test]
	fn test_get_row_settings_existing() {
		let mut txn = create_test_admin_transaction();
		let storage = StorageId::Table(TableId(42));
		let settings = RowSettings {
			ttl: Some(Ttl {
				duration: Duration::from_minutes(5).unwrap(),
				announce: false,
			}),
			persistent: true,
		};

		create_row_settings(&mut txn, storage, &settings).unwrap();

		let found = CatalogStore::get_row_settings(&mut Transaction::Admin(&mut txn), storage).unwrap();
		assert_eq!(found, settings);
	}

	#[test]
	fn test_get_row_settings_not_found_returns_error() {
		let mut txn = create_test_admin_transaction();
		let storage = StorageId::Table(TableId(999));

		let err = CatalogStore::get_row_settings(&mut Transaction::Admin(&mut txn), storage).unwrap_err();
		assert!(err.diagnostic().message.contains("row settings for storage"));
	}
}
