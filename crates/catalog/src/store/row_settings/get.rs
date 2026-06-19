// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::shape::ShapeId, internal, row::RowSettings};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::error::Error;

use crate::{CatalogStore, Result};

impl CatalogStore {
	#[allow(dead_code)]
	pub fn get_row_settings(rx: &mut Transaction<'_>, shape: ShapeId) -> Result<RowSettings> {
		Self::find_row_settings(rx, shape)?.ok_or_else(|| {
			Error(Box::new(internal!(
				"row settings for shape {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				shape
			)))
		})
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
	fn test_get_row_settings_existing() {
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

		let found = CatalogStore::get_row_settings(&mut Transaction::Admin(&mut txn), shape).unwrap();
		assert_eq!(found, settings);
	}

	#[test]
	fn test_get_row_settings_not_found_returns_error() {
		let mut txn = create_test_admin_transaction();
		let shape = ShapeId::Table(TableId(999));

		let err = CatalogStore::get_row_settings(&mut Transaction::Admin(&mut txn), shape).unwrap_err();
		assert!(err.diagnostic().message.contains("row settings for shape"));
	}
}
