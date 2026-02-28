// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::policy::PolicyId, key::policy::PolicyKey};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{
	CatalogStore, Result,
	store::policy::schema::policy::{ENABLED, ID, NAME, SCHEMA, TARGET_NAMESPACE, TARGET_OBJECT, TARGET_TYPE},
};

impl CatalogStore {
	pub(crate) fn alter_policy_enabled(
		txn: &mut AdminTransaction,
		policy_id: PolicyId,
		enabled: bool,
	) -> Result<()> {
		if let Some(def) = Self::find_policy(&mut Transaction::Admin(&mut *txn), policy_id)? {
			let mut row = SCHEMA.allocate();
			SCHEMA.set_u64(&mut row, ID, def.id);
			SCHEMA.set_utf8(&mut row, NAME, def.name.as_deref().unwrap_or(""));
			SCHEMA.set_utf8(&mut row, TARGET_TYPE, def.target_type.as_str());
			SCHEMA.set_utf8(&mut row, TARGET_NAMESPACE, def.target_namespace.as_deref().unwrap_or(""));
			SCHEMA.set_utf8(&mut row, TARGET_OBJECT, def.target_object.as_deref().unwrap_or(""));
			SCHEMA.set_bool(&mut row, ENABLED, enabled);

			txn.set(&PolicyKey::encoded(policy_id), row)?;
		}

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::catalog::policy::{PolicyTargetType, PolicyToCreate};
	use reifydb_engine::test_utils::create_test_admin_transaction;
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
