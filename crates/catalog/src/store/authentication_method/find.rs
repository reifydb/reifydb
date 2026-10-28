// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{
	interface::catalog::authentication_method::{AuthenticationMethod, AuthenticationMethodId},
	key::identity::AuthenticationMethodKey,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use crate::{
	CatalogStore, Result,
	store::authentication_method::{convert_authentication_method, shape::authentication_method},
};

impl CatalogStore {
	pub(crate) fn find_authentication_method(
		rx: &mut Transaction<'_>,
		id: AuthenticationMethodId,
	) -> Result<Option<AuthenticationMethod>> {
		rx.get(&[AuthenticationMethodKey::new(id).into()])?
			.pop()
			.flatten()
			.map(convert_authentication_method)
			.transpose()
	}

	pub(crate) fn find_authentication_method_by_name(
		rx: &mut Transaction<'_>,
		name: &str,
	) -> Result<Option<AuthenticationMethod>> {
		let stream = rx.range(AuthenticationMethodKey::full_scan(), RangeScope::All, 1024)?;

		for entry in stream {
			let multi = entry?;
			if authentication_method::get_name(EncodedCatalogRow::view(&multi.bytes)) == name {
				return Ok(Some(convert_authentication_method(multi)?));
			}
		}

		Ok(None)
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
	fn every_field_survives_the_row_round_trip() {
		// A ttl lost on the way back would make every session of the method immortal.
		let mut txn = create_test_admin_transaction();
		let created = CatalogStore::create_authentication_method(
			&mut txn,
			AuthenticationMethodToCreate {
				name: "github".to_string(),
				ttl: Some(Duration::from_minutes_const(43200)),
				promote_guest: true,
				properties: BTreeMap::from([
					("redirect_uri".to_string(), "https://app/cb".to_string()),
					("client_id".to_string(), "abc".to_string()),
				]),
			},
		)
		.unwrap();

		let found =
			CatalogStore::find_authentication_method_by_name(&mut Transaction::Admin(&mut txn), "github")
				.unwrap()
				.unwrap();
		assert_eq!(found, created);
		assert_eq!(found.ttl, Some(Duration::from_minutes_const(43200)));
	}

	#[test]
	fn a_missing_ttl_reads_back_as_forever() {
		let mut txn = create_test_admin_transaction();
		CatalogStore::create_authentication_method(
			&mut txn,
			AuthenticationMethodToCreate {
				name: "token".to_string(),
				ttl: None,
				promote_guest: false,
				properties: BTreeMap::new(),
			},
		)
		.unwrap();

		let found =
			CatalogStore::find_authentication_method_by_name(&mut Transaction::Admin(&mut txn), "token")
				.unwrap()
				.unwrap();
		assert_eq!(found.ttl, None);
	}

	#[test]
	fn an_unknown_name_is_not_found() {
		let mut txn = create_test_admin_transaction();
		let found =
			CatalogStore::find_authentication_method_by_name(&mut Transaction::Admin(&mut txn), "solana")
				.unwrap();
		assert!(found.is_none());
	}
}
