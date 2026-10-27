// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::authentication_method::{AuthenticationMethod, AuthenticationMethodToCreate},
	key::identity::AuthenticationMethodKey,
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::fragment::Fragment;

use crate::{
	CatalogStore, Result,
	error::{CatalogError, CatalogObjectKind},
	store::{authentication_method::encode_authentication_method, sequence::system::SystemSequence},
};

impl CatalogStore {
	pub(crate) fn create_authentication_method(
		txn: &mut AdminTransaction,
		to_create: AuthenticationMethodToCreate,
	) -> Result<AuthenticationMethod> {
		if Self::find_authentication_method_by_name(&mut Transaction::Admin(&mut *txn), &to_create.name)?
			.is_some()
		{
			return Err(CatalogError::AlreadyExists {
				kind: CatalogObjectKind::AuthenticationMethod,
				namespace: "system".to_string(),
				name: to_create.name,
				fragment: Fragment::None,
			}
			.into());
		}

		let method = AuthenticationMethod {
			id: SystemSequence::next_authentication_method_id(txn)?,
			name: to_create.name,
			ttl: to_create.ttl,
			promote_guest: to_create.promote_guest,
			properties: to_create.properties,
		};

		let row = encode_authentication_method(&method)?;
		txn.set(vec![(AuthenticationMethodKey::new(method.id).into(), row.freeze().into())])?;

		Ok(method)
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use reifydb_core::interface::catalog::authentication_method::AuthenticationMethodToCreate;
	use reifydb_test_harness::engine::create_test_admin_transaction;

	use crate::CatalogStore;

	fn password() -> AuthenticationMethodToCreate {
		AuthenticationMethodToCreate {
			name: "password".to_string(),
			ttl: None,
			promote_guest: false,
			properties: BTreeMap::new(),
		}
	}

	#[test]
	fn a_second_method_with_the_same_name_is_refused() {
		// Two rows for one provider would make the login gate pick either ttl.
		let mut txn = create_test_admin_transaction();
		let first = CatalogStore::create_authentication_method(&mut txn, password()).unwrap();
		let second = CatalogStore::create_authentication_method(&mut txn, password()).unwrap_err();

		assert_eq!(first.name, "password");
		assert_eq!(second.diagnostic().code, "CA_117");
	}

	#[test]
	fn ids_come_from_the_method_sequence() {
		let mut txn = create_test_admin_transaction();
		let a = CatalogStore::create_authentication_method(&mut txn, password()).unwrap();
		let mut token = password();
		token.name = "token".to_string();
		let b = CatalogStore::create_authentication_method(&mut txn, token).unwrap();

		assert_eq!(b.id, a.id + 1);
	}
}
