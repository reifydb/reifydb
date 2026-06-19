// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::shape::ShapeId, key::row_settings::RowSettingsKey, row::RowSettings};
use reifydb_transaction::transaction::Transaction;

use super::decode_row_settings;
use crate::{CatalogStore, Result};

impl CatalogStore {
	pub fn find_row_settings(rx: &mut Transaction<'_>, shape: ShapeId) -> Result<Option<RowSettings>> {
		let value = rx.get(&RowSettingsKey::encoded(shape))?;
		Ok(value.and_then(|v| decode_row_settings(&v.row)))
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::id::TableId,
		row::{RowSettings, Ttl, TtlCleanupMode},
	};
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use super::*;
	use crate::store::row_settings::create::create_row_settings;

	#[test]
	fn test_find_row_settings_existing() {
		let mut txn = create_test_admin_transaction();
		let shape = ShapeId::Table(TableId(42));
		let settings = RowSettings {
			ttl: Some(Ttl {
				duration_nanos: 300_000_000_000,
				cleanup_mode: TtlCleanupMode::Drop,
			}),
			persistent: true,
		};

		create_row_settings(&mut txn, shape, &settings).unwrap();

		let found = CatalogStore::find_row_settings(&mut Transaction::Admin(&mut txn), shape).unwrap();
		assert_eq!(found, Some(settings));
	}

	#[test]
	fn test_find_row_settings_not_found() {
		let mut txn = create_test_admin_transaction();
		let shape = ShapeId::Table(TableId(999));

		let found = CatalogStore::find_row_settings(&mut Transaction::Admin(&mut txn), shape).unwrap();
		assert_eq!(found, None);
	}
}
