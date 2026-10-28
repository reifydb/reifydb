// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::authentication_method::AuthenticationMethodId, key::identity::AuthenticationMethodKey,
};
use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn drop_authentication_method(
		txn: &mut AdminTransaction,
		method: AuthenticationMethodId,
	) -> Result<()> {
		txn.remove(vec![(AuthenticationMethodKey::new(method).into(), None)])?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use reifydb_core::interface::catalog::authentication_method::AuthenticationMethodToCreate;
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::CatalogStore;

	#[test]
	fn a_dropped_method_is_gone() {
		let mut txn = create_test_admin_transaction();
		let method = CatalogStore::create_authentication_method(
			&mut txn,
			AuthenticationMethodToCreate {
				name: "password".to_string(),
				ttl: None,
				promote_guest: false,
				properties: BTreeMap::new(),
			},
		)
		.unwrap();

		CatalogStore::drop_authentication_method(&mut txn, method.id).unwrap();

		let found =
			CatalogStore::find_authentication_method(&mut Transaction::Admin(&mut txn), method.id).unwrap();
		assert!(found.is_none());
	}
}
