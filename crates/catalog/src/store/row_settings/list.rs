// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::shape::ShapeId,
	key::{
		EncodableKey,
		row_settings::{RowSettingsKey, RowSettingsKeyRange},
	},
	row::RowSettings,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use super::decode_row_settings;
use crate::{CatalogStore, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowSettingsEntry {
	pub shape: ShapeId,
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
				&& let Some(settings) = decode_row_settings(&entry.row)
			{
				result.push(RowSettingsEntry {
					shape: key.shape,
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
		interface::catalog::id::{RingBufferId, SeriesId, TableId},
		row::{RowSettings, Ttl, TtlAnchor, TtlCleanupMode},
	};
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

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

		let table_shape = ShapeId::Table(TableId(1));
		let rb_shape = ShapeId::RingBuffer(RingBufferId(2));
		let series_shape = ShapeId::Series(SeriesId(3));

		let settings_table = RowSettings {
			ttl: Some(Ttl {
				duration_nanos: 300_000_000_000,
				anchor: TtlAnchor::Created,
				cleanup_mode: TtlCleanupMode::Drop,
			}),
			persistent: true,
		};
		let settings_rb = RowSettings {
			ttl: Some(Ttl {
				duration_nanos: 600_000_000_000,
				anchor: TtlAnchor::Updated,
				cleanup_mode: TtlCleanupMode::Delete,
			}),
			persistent: false,
		};
		let settings_series = RowSettings {
			ttl: Some(Ttl {
				duration_nanos: 86_400_000_000_000,
				anchor: TtlAnchor::Created,
				cleanup_mode: TtlCleanupMode::Drop,
			}),
			persistent: true,
		};

		create_row_settings(&mut txn, table_shape, &settings_table).unwrap();
		create_row_settings(&mut txn, rb_shape, &settings_rb).unwrap();
		create_row_settings(&mut txn, series_shape, &settings_series).unwrap();

		let entries = CatalogStore::list_row_settings(&mut Transaction::Admin(&mut txn)).unwrap();
		assert_eq!(entries.len(), 3);
		assert!(entries.iter().any(|e| e.shape == table_shape && e.settings == settings_table));
		assert!(entries.iter().any(|e| e.shape == rb_shape && e.settings == settings_rb));
		assert!(entries.iter().any(|e| e.shape == series_shape && e.settings == settings_series));
	}
}
