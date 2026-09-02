// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{interface::catalog::storage::StorageId, key::row::RowSettingsKey, row::RowSettings};
use reifydb_transaction::transaction::Transaction;

use super::decode_row_settings;
use crate::{CatalogStore, Result};

impl CatalogStore {
	pub fn find_row_settings(rx: &mut Transaction<'_>, storage: StorageId) -> Result<Option<RowSettings>> {
		let value = rx.get(&RowSettingsKey::encoded(storage))?;
		Ok(value.and_then(|v| decode_row_settings(EncodedCatalogRow::view(&v.bytes))))
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
	fn test_find_row_settings_existing() {
		let mut txn = create_test_admin_transaction();
		let storage = StorageId::Table(TableId(42));
		let settings = RowSettings {
			ttl: Some(Ttl {
				duration: Duration::from_minutes(5).unwrap(),
			}),
			persistent: true,
		};

		create_row_settings(&mut txn, storage, &settings).unwrap();

		let found = CatalogStore::find_row_settings(&mut Transaction::Admin(&mut txn), storage).unwrap();
		assert_eq!(found, Some(settings));
	}

	#[test]
	fn test_find_row_settings_not_found() {
		let mut txn = create_test_admin_transaction();
		let storage = StorageId::Table(TableId(999));

		let found = CatalogStore::find_row_settings(&mut Transaction::Admin(&mut txn), storage).unwrap();
		assert_eq!(found, None);
	}
}
