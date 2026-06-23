// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{change::CatalogTrackRowSettingsChangeOperations, shape::ShapeId},
	key::row_settings::RowSettingsKey,
	row::RowSettings,
};
use reifydb_transaction::transaction::admin::AdminTransaction;

use super::encode_row_settings;
use crate::Result;

pub fn create_row_settings(txn: &mut AdminTransaction, shape: ShapeId, settings: &RowSettings) -> Result<()> {
	let value = encode_row_settings(settings);
	txn.set(&RowSettingsKey::encoded(shape), value)?;
	txn.track_row_settings_created(shape, settings.clone())?;
	Ok(())
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::id::{RingBufferId, SeriesId, TableId},
		row::{RowSettings, Ttl, TtlCleanupMode},
	};
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::value::duration::Duration;

	use super::*;
	use crate::CatalogStore;

	#[test]
	fn test_create_row_settings_for_table() {
		let mut txn = create_test_admin_transaction();
		let shape = ShapeId::Table(TableId(42));
		let settings = RowSettings {
			ttl: Some(Ttl {
				duration: Duration::from_minutes(5).unwrap(),
				cleanup_mode: TtlCleanupMode::Drop,
			}),
			persistent: true,
		};

		create_row_settings(&mut txn, shape, &settings).unwrap();

		let found = CatalogStore::find_row_settings(&mut Transaction::Admin(&mut txn), shape)
			.unwrap()
			.expect("row settings should be stored");
		assert_eq!(found, settings);
	}

	#[test]
	fn test_create_row_settings_non_persistent() {
		let mut txn = create_test_admin_transaction();
		let shape = ShapeId::Table(TableId(43));
		let settings = RowSettings {
			ttl: Some(Ttl {
				duration: Duration::from_minutes(1).unwrap(),
				cleanup_mode: TtlCleanupMode::Drop,
			}),
			persistent: false,
		};

		create_row_settings(&mut txn, shape, &settings).unwrap();

		let found = CatalogStore::find_row_settings(&mut Transaction::Admin(&mut txn), shape)
			.unwrap()
			.expect("row settings should be stored");
		assert_eq!(found, settings);
		assert!(!found.persistent);
	}

	#[test]
	fn test_create_row_settings_for_ringbuffer() {
		let mut txn = create_test_admin_transaction();
		let shape = ShapeId::RingBuffer(RingBufferId(200));
		let settings = RowSettings {
			ttl: Some(Ttl {
				duration: Duration::from_hours(1).unwrap(),
				cleanup_mode: TtlCleanupMode::Delete,
			}),
			persistent: true,
		};

		create_row_settings(&mut txn, shape, &settings).unwrap();

		let found = CatalogStore::find_row_settings(&mut Transaction::Admin(&mut txn), shape)
			.unwrap()
			.expect("row settings should be stored");
		assert_eq!(found, settings);
	}

	#[test]
	fn test_create_row_settings_for_series() {
		let mut txn = create_test_admin_transaction();
		let shape = ShapeId::Series(SeriesId(7));
		let settings = RowSettings {
			ttl: Some(Ttl {
				duration: Duration::from_days(1).unwrap(),
				cleanup_mode: TtlCleanupMode::Drop,
			}),
			persistent: true,
		};

		create_row_settings(&mut txn, shape, &settings).unwrap();

		let found = CatalogStore::find_row_settings(&mut Transaction::Admin(&mut txn), shape)
			.unwrap()
			.expect("row settings should be stored");
		assert_eq!(found, settings);
	}

	#[test]
	fn test_create_row_settings_overwrite() {
		let mut txn = create_test_admin_transaction();
		let shape = ShapeId::Table(TableId(42));
		let settings_v1 = RowSettings {
			ttl: Some(Ttl {
				duration: Duration::from_minutes(5).unwrap(),
				cleanup_mode: TtlCleanupMode::Drop,
			}),
			persistent: true,
		};
		let settings_v2 = RowSettings {
			ttl: Some(Ttl {
				duration: Duration::from_minutes(10).unwrap(),
				cleanup_mode: TtlCleanupMode::Delete,
			}),
			persistent: false,
		};

		create_row_settings(&mut txn, shape, &settings_v1).unwrap();
		create_row_settings(&mut txn, shape, &settings_v2).unwrap();

		let found = CatalogStore::find_row_settings(&mut Transaction::Admin(&mut txn), shape)
			.unwrap()
			.expect("row settings should be stored");
		assert_eq!(found, settings_v2);
	}
}
