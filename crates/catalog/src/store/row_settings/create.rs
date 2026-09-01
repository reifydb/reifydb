// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{change::CatalogTrackRowSettingsChangeOperations, storage::StorageId},
	key::row::RowSettingsKey,
	row::RowSettings,
};
use reifydb_transaction::transaction::admin::AdminTransaction;

use super::encode_row_settings;
use crate::Result;

pub fn create_row_settings(txn: &mut AdminTransaction, storage: StorageId, settings: &RowSettings) -> Result<()> {
	let value = encode_row_settings(settings);
	txn.set(&RowSettingsKey::encoded(storage), value)?;
	txn.track_row_settings_created(storage, settings.clone())?;
	Ok(())
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
	use crate::CatalogStore;

	#[test]
	fn test_create_row_settings_for_table() {
		let mut txn = create_test_admin_transaction();
		let storage = StorageId::Table(TableId(42));
		let settings = RowSettings {
			ttl: Some(Ttl {
				duration: Duration::from_minutes(5).unwrap(),
			}),
			persistent: true,
		};

		create_row_settings(&mut txn, storage, &settings).unwrap();

		let found = CatalogStore::find_row_settings(&mut Transaction::Admin(&mut txn), storage)
			.unwrap()
			.expect("row settings should be stored");
		assert_eq!(found, settings);
	}

	#[test]
	fn test_create_row_settings_non_persistent() {
		let mut txn = create_test_admin_transaction();
		let storage = StorageId::Table(TableId(43));
		let settings = RowSettings {
			ttl: Some(Ttl {
				duration: Duration::from_minutes(1).unwrap(),
			}),
			persistent: false,
		};

		create_row_settings(&mut txn, storage, &settings).unwrap();

		let found = CatalogStore::find_row_settings(&mut Transaction::Admin(&mut txn), storage)
			.unwrap()
			.expect("row settings should be stored");
		assert_eq!(found, settings);
		assert!(!found.persistent);
	}

	#[test]
	fn test_create_row_settings_for_ringbuffer() {
		let mut txn = create_test_admin_transaction();
		let storage = StorageId::RingBuffer(RingBufferId(200));
		let settings = RowSettings {
			ttl: Some(Ttl {
				duration: Duration::from_hours(1).unwrap(),
			}),
			persistent: true,
		};

		create_row_settings(&mut txn, storage, &settings).unwrap();

		let found = CatalogStore::find_row_settings(&mut Transaction::Admin(&mut txn), storage)
			.unwrap()
			.expect("row settings should be stored");
		assert_eq!(found, settings);
	}

	#[test]
	fn test_create_row_settings_for_series() {
		let mut txn = create_test_admin_transaction();
		let storage = StorageId::Series(SeriesId(7));
		let settings = RowSettings {
			ttl: Some(Ttl {
				duration: Duration::from_days(1).unwrap(),
			}),
			persistent: true,
		};

		create_row_settings(&mut txn, storage, &settings).unwrap();

		let found = CatalogStore::find_row_settings(&mut Transaction::Admin(&mut txn), storage)
			.unwrap()
			.expect("row settings should be stored");
		assert_eq!(found, settings);
	}

	#[test]
	fn test_create_row_settings_overwrite() {
		let mut txn = create_test_admin_transaction();
		let storage = StorageId::Table(TableId(42));
		let settings_v1 = RowSettings {
			ttl: Some(Ttl {
				duration: Duration::from_minutes(5).unwrap(),
			}),
			persistent: true,
		};
		let settings_v2 = RowSettings {
			ttl: Some(Ttl {
				duration: Duration::from_minutes(10).unwrap(),
			}),
			persistent: false,
		};

		create_row_settings(&mut txn, storage, &settings_v1).unwrap();
		create_row_settings(&mut txn, storage, &settings_v2).unwrap();

		let found = CatalogStore::find_row_settings(&mut Transaction::Admin(&mut txn), storage)
			.unwrap()
			.expect("row settings should be stored");
		assert_eq!(found, settings_v2);
	}
}
