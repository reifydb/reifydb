// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::authentication_method::AuthenticationMethod, key::identity::AuthenticationMethodKey,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use crate::{CatalogStore, Result, store::authentication_method::convert_authentication_method};

impl CatalogStore {
	pub(crate) fn list_all_authentication_methods(rx: &mut Transaction<'_>) -> Result<Vec<AuthenticationMethod>> {
		let mut result = Vec::new();
		let stream = rx.range(AuthenticationMethodKey::full_scan(), RangeScope::All, 1024)?;

		for entry in stream {
			result.push(convert_authentication_method(entry?)?);
		}

		Ok(result)
	}
}
