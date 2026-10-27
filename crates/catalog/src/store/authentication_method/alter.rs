// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::authentication_method::AuthenticationMethod, key::identity::AuthenticationMethodKey,
};
use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::{CatalogStore, Result, store::authentication_method::encode_authentication_method};

impl CatalogStore {
	pub(crate) fn alter_authentication_method(
		txn: &mut AdminTransaction,
		method: &AuthenticationMethod,
	) -> Result<()> {
		let row = encode_authentication_method(method)?;
		txn.set(vec![(AuthenticationMethodKey::new(method.id).into(), row.freeze().into())])?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use reifydb_core::interface::catalog::authentication_method::AuthenticationMethodToCreate;
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::value::duration::Duration;

	use crate::CatalogStore;

	#[test]
	fn alter_replaces_the_whole_row() {
		// The handler passes the merged row, so the store must not keep any old field.
		let mut txn = create_test_admin_transaction();
		let mut method = CatalogStore::create_authentication_method(
			&mut txn,
			AuthenticationMethodToCreate {
				name: "github".to_string(),
				ttl: Some(Duration::from_minutes_const(60)),
				promote_guest: false,
				properties: BTreeMap::from([("client_id".to_string(), "old".to_string())]),
			},
		)
		.unwrap();

		method.ttl = None;
		method.promote_guest = true;
		method.properties = BTreeMap::from([("client_id".to_string(), "new".to_string())]);
		CatalogStore::alter_authentication_method(&mut txn, &method).unwrap();

		let found = CatalogStore::find_authentication_method(&mut Transaction::Admin(&mut txn), method.id)
			.unwrap()
			.unwrap();
		assert_eq!(found, method);
	}
}
