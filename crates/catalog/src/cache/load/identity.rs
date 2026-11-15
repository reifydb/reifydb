// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::key::identity::IdentityKey;
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::error::ValueError;

use super::CatalogCache;
use crate::store::identity::convert_identity;

pub(crate) fn load_identities(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<(), ValueError> {
	let range = IdentityKey::full_scan();
	let stream = rx.range(range, RangeScope::All, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;
		let identity = convert_identity(multi)?;
		catalog.set_identity(identity.id, version, Some(identity));
	}

	Ok(())
}
