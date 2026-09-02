// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{
	interface::catalog::storage::StorageId,
	key::{
		row::{RowSettingsKey, RowSettingsKeyRange},
		typed::key::Key,
	},
	row::RowSettings,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use super::decode_row_settings;
use crate::{CatalogStore, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowSettingsEntry {
	pub storage: StorageId,
	pub settings: RowSettings,
}

impl CatalogStore {
	#[allow(dead_code)]
	pub fn list_row_settings(rx: &mut Transaction<'_>) -> Result<Vec<RowSettingsEntry>> {
		let mut result = Vec::new();

		let stream = rx.range(RowSettingsKeyRange::full_scan(), RangeScope::All, 1024)?;

		for entry in stream {
			let entry = entry?;
			if let Some(key) = RowSettingsKey::decode(&entry.key)
				&& let Some(settings) = decode_row_settings(EncodedCatalogRow::view(&entry.bytes))
			{
				result.push(RowSettingsEntry {
					storage: key.storage,
					settings,
				});
			}
		}

		Ok(result)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::{
			id::{RingBufferId, SeriesId, TableId},
			storage::StorageId,
		},
		row::{RowSettings, Ttl},
	};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::value::duration::Duration;

	use super::*;
	use crate::store::row_settings::create::create_row_settings;

	#[test]
	fn test_list_row_settings_empty() {
		let mut txn = create_test_admin_transaction();

		let entries = CatalogStore::list_row_settings(&mut Transaction::Admin(&mut txn)).unwrap();
		assert!(entries.is_empty());
	}

	#[test]
	fn test_list_row_settings_multiple() {
		let mut txn = create_test_admin_transaction();

		let table_storage = StorageId::Table(TableId(1));
		let rb_storage = StorageId::RingBuffer(RingBufferId(2));
		let series_storage = StorageId::Series(SeriesId(3));

		let settings_table = RowSettings {
			ttl: Some(Ttl {
				duration: Duration::from_minutes(5).unwrap(),
			}),
			persistent: true,
		};
		let settings_rb = RowSettings {
			ttl: Some(Ttl {
				duration: Duration::from_minutes(10).unwrap(),
			}),
			persistent: false,
		};
		let settings_series = RowSettings {
			ttl: Some(Ttl {
				duration: Duration::from_days(1).unwrap(),
			}),
			persistent: true,
		};

		create_row_settings(&mut txn, table_storage, &settings_table).unwrap();
		create_row_settings(&mut txn, rb_storage, &settings_rb).unwrap();
		create_row_settings(&mut txn, series_storage, &settings_series).unwrap();

		let entries = CatalogStore::list_row_settings(&mut Transaction::Admin(&mut txn)).unwrap();
		assert_eq!(entries.len(), 3);
		assert!(entries.iter().any(|e| e.storage == table_storage && e.settings == settings_table));
		assert!(entries.iter().any(|e| e.storage == rb_storage && e.settings == settings_rb));
		assert!(entries.iter().any(|e| e.storage == series_storage && e.settings == settings_series));
	}
}
