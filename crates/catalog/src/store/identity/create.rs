// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::identity::IdentityDef, key::identity::IdentityKey};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::{fragment::Fragment, value::identity::IdentityId};

use crate::{
	CatalogStore, Result,
	error::{CatalogError, CatalogObjectKind},
	store::identity::schema::identity::{ENABLED, IDENTITY, NAME, SCHEMA},
};

impl CatalogStore {
	/// Create an identity with a specific IdentityId. Used for bootstrapping system identities (e.g. root).
	/// Skips duplicate check — caller must ensure uniqueness.
	pub(crate) fn create_identity_with_id(
		txn: &mut AdminTransaction,
		name: &str,
		id: IdentityId,
	) -> Result<IdentityDef> {
		let mut row = SCHEMA.allocate();
		SCHEMA.set_identity_id(&mut row, IDENTITY, id);
		SCHEMA.set_utf8(&mut row, NAME, name);
		SCHEMA.set_bool(&mut row, ENABLED, true);

		txn.set(&IdentityKey::encoded(id), row)?;

		Ok(IdentityDef {
			id,
			name: name.to_string(),
			enabled: true,
		})
	}

	pub(crate) fn create_identity(txn: &mut AdminTransaction, name: &str) -> Result<IdentityDef> {
		if let Some(_) = Self::find_identity_by_name(&mut Transaction::Admin(&mut *txn), name)? {
			return Err(CatalogError::AlreadyExists {
				kind: CatalogObjectKind::Identity,
				namespace: "system".to_string(),
				name: name.to_string(),
				fragment: Fragment::None,
			}
			.into());
		}

		let id = IdentityId::generate();

		let mut row = SCHEMA.allocate();
		SCHEMA.set_identity_id(&mut row, IDENTITY, id);
		SCHEMA.set_utf8(&mut row, NAME, name);
		SCHEMA.set_bool(&mut row, ENABLED, true);

		txn.set(&IdentityKey::encoded(id), row)?;

		Ok(IdentityDef {
			id,
			name: name.to_string(),
			enabled: true,
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_utils::create_test_admin_transaction;

	use crate::CatalogStore;

	#[test]
	fn test_create_identity() {
		let mut txn = create_test_admin_transaction();
		let identity = CatalogStore::create_identity(&mut txn, "alice").unwrap();
		assert_eq!(identity.name, "alice");
		assert!(identity.enabled);
	}

	#[test]
	fn test_create_identity_duplicate() {
		let mut txn = create_test_admin_transaction();
		CatalogStore::create_identity(&mut txn, "alice").unwrap();
		let err = CatalogStore::create_identity(&mut txn, "alice").unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_040");
	}
}
