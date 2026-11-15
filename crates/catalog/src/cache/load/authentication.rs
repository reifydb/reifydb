// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::key::identity::AuthenticationKey;
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::error::ValueError;

use super::CatalogCache;
use crate::store::authentication::convert_authentication;

pub(crate) fn load_authentications(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<(), ValueError> {
	let range = AuthenticationKey::full_scan();
	let stream = rx.range(range, RangeScope::All, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;
		let auth = convert_authentication(multi)?;
		catalog.set_authentication(auth.id, version, Some(auth));
	}

	Ok(())
}
