// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::object::ObjectId, internal, row::RowSettings};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::error::Error;

use crate::{CatalogStore, Result};

impl CatalogStore {
	#[allow(dead_code)]
	pub fn get_row_settings(rx: &mut Transaction<'_>, object: ObjectId) -> Result<RowSettings> {
		Self::find_row_settings(rx, object)?.ok_or_else(|| {
			Error(Box::new(internal!(
				"row settings for object {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				object
			)))
		})
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::id::TableId,
		row::{RowSettings, Ttl},
	};
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::value::duration::Duration;

	use super::*;
	use crate::store::row_settings::create::create_row_settings;

	#[test]
	fn test_get_row_settings_existing() {
		let mut txn = create_test_admin_transaction();
		let object = ObjectId::Table(TableId(42));
		let settings = RowSettings {
			ttl: Some(Ttl {
				duration: Duration::from_minutes(5).unwrap(),
				announce: false,
			}),
			persistent: true,
		};

		create_row_settings(&mut txn, object, &settings).unwrap();

		let found = CatalogStore::get_row_settings(&mut Transaction::Admin(&mut txn), object).unwrap();
		assert_eq!(found, settings);
	}

	#[test]
	fn test_get_row_settings_not_found_returns_error() {
		let mut txn = create_test_admin_transaction();
		let object = ObjectId::Table(TableId(999));

		let err = CatalogStore::get_row_settings(&mut Transaction::Admin(&mut txn), object).unwrap_err();
		assert!(err.diagnostic().message.contains("row settings for object"));
	}
}
