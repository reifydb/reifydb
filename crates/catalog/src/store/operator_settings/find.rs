// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::flow::FlowNodeId, key::operator_settings::OperatorSettingsKey, row::OperatorSettings,
};
use reifydb_transaction::transaction::Transaction;

use super::decode_operator_settings;
use crate::{CatalogStore, Result};

impl CatalogStore {
	pub fn find_operator_settings(
		rx: &mut Transaction<'_>,
		operator: FlowNodeId,
	) -> Result<Option<OperatorSettings>> {
		let value = rx.get(&OperatorSettingsKey::encoded(operator))?;
		Ok(value.and_then(|v| decode_operator_settings(&v.row)))
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::flow::FlowNodeId,
		row::{OperatorSettings, Ttl, TtlAnchor, TtlCleanupMode},
	};
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use super::*;
	use crate::store::operator_settings::create::create_operator_settings;

	#[test]
	fn test_find_operator_settings_existing() {
		let mut txn = create_test_admin_transaction();
		let operator = FlowNodeId(42);
		let settings = OperatorSettings {
			ttl: Some(Ttl {
				duration_nanos: 300_000_000_000,
				anchor: TtlAnchor::Created,
				cleanup_mode: TtlCleanupMode::Drop,
			}),
			join: None,
		};

		create_operator_settings(&mut txn, operator, &settings).unwrap();

		let found = CatalogStore::find_operator_settings(&mut Transaction::Admin(&mut txn), operator).unwrap();
		assert_eq!(found, Some(settings));
	}

	#[test]
	fn test_find_operator_settings_not_found() {
		let mut txn = create_test_admin_transaction();
		let operator = FlowNodeId(999);

		let found = CatalogStore::find_operator_settings(&mut Transaction::Admin(&mut txn), operator).unwrap();
		assert_eq!(found, None);
	}
}
