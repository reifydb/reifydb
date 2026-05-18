// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::policy::PolicyId, key::policy::PolicyKey};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{
	CatalogStore, Result,
	store::policy::shape::policy::{ENABLED, ID, NAME, SHAPE, TARGET_NAMESPACE, TARGET_SHAPE, TARGET_TYPE},
};

impl CatalogStore {
	pub(crate) fn alter_policy_enabled(
		txn: &mut AdminTransaction,
		policy_id: PolicyId,
		enabled: bool,
	) -> Result<()> {
		if let Some(def) = Self::find_policy(&mut Transaction::Admin(&mut *txn), policy_id)? {
			let mut row = SHAPE.allocate();
			SHAPE.set_u64(&mut row, ID, def.id);
			SHAPE.set_utf8(&mut row, NAME, def.name.as_deref().unwrap_or(""));
			SHAPE.set_utf8(&mut row, TARGET_TYPE, def.target_type.as_str());
			SHAPE.set_utf8(&mut row, TARGET_NAMESPACE, def.target_namespace.as_deref().unwrap_or(""));
			SHAPE.set_utf8(&mut row, TARGET_SHAPE, def.target_shape.as_deref().unwrap_or(""));
			SHAPE.set_bool(&mut row, ENABLED, enabled);

			txn.set(&PolicyKey::encoded(policy_id), row)?;
		}

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::catalog::policy::{PolicyTargetType, PolicyToCreate};
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::CatalogStore;

	#[test]
	fn test_alter_policy_enabled() {
		let mut txn = create_test_admin_transaction();
		let to_create = PolicyToCreate {
			name: Some("test_policy".to_string()),
			target_type: PolicyTargetType::Table,
			target_namespace: None,
			target_shape: None,
			operations: vec![],
		};
		let (def, _) = CatalogStore::create_policy(&mut txn, to_create).unwrap();
		assert!(def.enabled);

		CatalogStore::alter_policy_enabled(&mut txn, def.id, false).unwrap();
		let found = CatalogStore::find_policy(&mut Transaction::Admin(&mut txn), def.id).unwrap().unwrap();
		assert!(!found.enabled);

		CatalogStore::alter_policy_enabled(&mut txn, def.id, true).unwrap();
		let found = CatalogStore::find_policy(&mut Transaction::Admin(&mut txn), def.id).unwrap().unwrap();
		assert!(found.enabled);
	}
}
