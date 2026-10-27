// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::{EncodedCatalogRow, EncodedCatalogRowBuilder};
use reifydb_core::interface::{catalog::authentication_method::AuthenticationMethod, store::MultiVersionRow};
use serde_json::{from_str, to_string};

use crate::{Result, error::CatalogError, store::authentication_method::shape::authentication_method};

pub mod alter;
pub mod create;
pub mod drop;
pub mod find;
pub mod list;
pub mod shape;

pub(crate) fn convert_authentication_method<K>(multi: MultiVersionRow<K>) -> Result<AuthenticationMethod> {
	let bytes = EncodedCatalogRow::try_from(multi.bytes)?;
	let name = authentication_method::get_name(&bytes).to_string();
	let properties = from_str(authentication_method::get_properties(&bytes)).map_err(|e| {
		CatalogError::AuthenticationMethodProperties {
			method: name.clone(),
			reason: e.to_string(),
		}
	})?;

	Ok(AuthenticationMethod {
		id: authentication_method::get_id(&bytes),
		name,
		ttl: authentication_method::try_get_ttl(&bytes),
		promote_guest: authentication_method::get_promote_guest(&bytes),
		properties,
	})
}

pub(crate) fn encode_authentication_method(method: &AuthenticationMethod) -> Result<EncodedCatalogRowBuilder> {
	let properties = to_string(&method.properties).map_err(|e| CatalogError::AuthenticationMethodProperties {
		method: method.name.clone(),
		reason: e.to_string(),
	})?;

	let mut row = authentication_method::allocate();
	authentication_method::set_id(&mut row, method.id);
	authentication_method::set_name(&mut row, &method.name);
	if let Some(ttl) = method.ttl {
		authentication_method::set_ttl(&mut row, ttl);
	}
	authentication_method::set_promote_guest(&mut row, method.promote_guest);
	authentication_method::set_properties(&mut row, &properties);
	Ok(row)
}

#[cfg(test)]
mod tests {
	use reifydb_core::key::identity::AuthenticationMethodKey;
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::{CatalogStore, store::authentication_method::shape::authentication_method};

	#[test]
	fn corrupt_properties_fail_loud_instead_of_reading_as_empty() {
		// Empty props would turn a corrupt github row into a misleading missing client_id.
		let mut txn = create_test_admin_transaction();
		let mut row = authentication_method::allocate();
		authentication_method::set_id(&mut row, 7);
		authentication_method::set_name(&mut row, "github");
		authentication_method::set_promote_guest(&mut row, false);
		authentication_method::set_properties(&mut row, "not json");
		txn.set(vec![(AuthenticationMethodKey::new(7).into(), row.freeze().into())]).unwrap();

		let err = CatalogStore::find_authentication_method(&mut Transaction::Admin(&mut txn), 7).unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_124");
	}
}
