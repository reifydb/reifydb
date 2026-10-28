// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::key::identity::AuthenticationMethodKey;
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use super::CatalogCache;
use crate::{Result, store::authentication_method::convert_authentication_method};

pub(crate) fn load_authentication_methods(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let stream = rx.range(AuthenticationMethodKey::full_scan(), RangeScope::All, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;
		let method = convert_authentication_method(multi)?;
		catalog.set_authentication_method(method.id, version, Some(method));
	}

	Ok(())
}
