// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::policy::PolicyId, key::identity::PolicyKey};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{CatalogStore, Result, store::policy::shape::policy};

impl CatalogStore {
	pub(crate) fn alter_policy_enabled(
		txn: &mut AdminTransaction,
		policy_id: PolicyId,
		enabled: bool,
	) -> Result<()> {
		if let Some(def) = Self::find_policy(&mut Transaction::Admin(&mut *txn), policy_id)? {
			let mut row = policy::allocate();
			policy::set_id(&mut row, def.id);
			policy::set_name(&mut row, def.name.as_deref().unwrap_or(""));
			policy::set_target_type(&mut row, def.target_type.as_str());
			policy::set_target_namespace(&mut row, def.target_namespace.as_deref().unwrap_or(""));
			policy::set_target_object(&mut row, def.target_object.as_deref().unwrap_or(""));
			policy::set_enabled(&mut row, enabled);

			txn.set(&PolicyKey::encoded(policy_id), row.freeze())?;
		}

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::catalog::policy::{PolicyTargetType, PolicyToCreate};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::CatalogStore;

	#[test]
	fn test_alter_policy_enabled() {
		let mut txn = create_test_admin_transaction();
		let to_create = PolicyToCreate {
			name: Some("test_policy".to_string()),
			target_type: PolicyTargetType::Table,
			target_namespace: None,
			target_object: None,
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
